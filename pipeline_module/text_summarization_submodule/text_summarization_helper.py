import json
import csv
import os
import re
from typing import List, Dict, Any, Optional
import warnings
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from transformers import pipeline, AutoTokenizer
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_folder_name, SCENE_SEGMENTED_FILE_CSV, SUMMARIZED_SCENES
from ..utils_module.timeit_decorator import timeit

# Suppress the specific warning
warnings.filterwarnings("ignore", message=".*clean_up_tokenization_spaces.*")


class TextSummarization:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.tokenizer = AutoTokenizer.from_pretrained(
            "facebook/bart-large-cnn",
            clean_up_tokenization_spaces=True
        )
        self.summarizer = pipeline(
            "summarization",
            model="facebook/bart-large-cnn",
            tokenizer=self.tokenizer
        )

        # Initialize with default values that will be adjusted
        self.MIN_SCENE_DURATION = 5.0  # seconds
        self.MIN_TEXT_LENGTH = 50  # characters
        self.DEFAULT_SUMMARY_LENGTH = 130

        # Calculate adaptive parameters based on video characteristics
        self._calculate_adaptive_parameters()

    def select_quality_captions(self, rated_captions, video_duration):
        """
        Select high-quality captions based on ratings and video characteristics.

        Arguments:
            rated_captions: List of captions with rating information
            video_duration: Duration of the video in seconds

        Returns:
            List of selected high-quality captions
        """
        if not rated_captions:
            return []

        try:
            # Set quality threshold based on video length
            # Short videos need higher quality captions since there are fewer of them
            if video_duration < 60:  # Short videos (<1 min)
                quality_threshold = 0.65
            elif video_duration < 300:  # Medium videos (1-5 min)
                quality_threshold = 0.55
            else:  # Longer videos
                quality_threshold = 0.45

            self.logger.info(f"Using caption quality threshold: {quality_threshold} for {video_duration}s video")

            # Filter by threshold first
            quality_captions = [cap for cap in rated_captions
                                if float(cap.get('rating', 0)) > quality_threshold]

            # Calculate minimum number of captions needed based on video duration
            min_captions_needed = max(3, int(video_duration / 15))

            # Fallback if too few captions meet threshold
            if len(quality_captions) < min_captions_needed:
                self.logger.info(
                    f"Too few captions ({len(quality_captions)}) meet quality threshold. Falling back to top {min_captions_needed} captions.")
                # Sort by rating and take top needed captions
                sorted_captions = sorted(rated_captions,
                                         key=lambda x: float(x.get('rating', 0)),
                                         reverse=True)
                quality_captions = sorted_captions[:min_captions_needed]

            # Remove near-duplicate captions (those with very similar text)
            unique_captions = self._remove_duplicate_captions(quality_captions)

            self.logger.info(f"Selected {len(unique_captions)} quality captions from {len(rated_captions)} available")
            return unique_captions

        except Exception as e:
            self.logger.error(f"Error selecting quality captions: {str(e)}")
            # Fall back to simple sorting by rating
            sorted_captions = sorted(rated_captions,
                                     key=lambda x: float(x.get('rating', 0)),
                                     reverse=True)
            # Take at least 3 captions or 25% of available ones, whichever is greater
            return sorted_captions[:max(3, len(sorted_captions) // 4)]

    def _remove_duplicate_captions(self, captions):
        """
        Remove near-duplicate captions that convey the same information.
        Uses a simple similarity heuristic based on word overlap.

        Arguments:
            captions: List of caption dictionaries

        Returns:
            List of captions with duplicates removed
        """
        if not captions:
            return []

        try:
            unique_captions = []
            seen_texts = set()

            for cap in captions:
                # Normalize text for comparison (lowercase, remove extra spaces)
                caption_text = cap.get('caption', '')
                if not caption_text:
                    continue

                normalized_text = ' '.join(caption_text.lower().split())

                # Simple duplicate detection based on character overlap
                is_duplicate = False
                for seen_text in seen_texts:
                    # If 80% of words overlap, consider it a duplicate
                    words1 = set(normalized_text.split())
                    words2 = set(seen_text.split())

                    if len(words1) == 0 or len(words2) == 0:
                        continue

                    overlap = len(words1.intersection(words2))
                    similarity = overlap / min(len(words1), len(words2))

                    if similarity > 0.8:
                        is_duplicate = True
                        break

                if not is_duplicate:
                    seen_texts.add(normalized_text)
                    unique_captions.append(cap)

            return unique_captions

        except Exception as e:
            self.logger.error(f"Error removing duplicate captions: {str(e)}")
            return captions  # Return original captions if error occurs

    def _calculate_adaptive_parameters(self):
        """Calculate adaptive parameters based on video characteristics."""
        try:
            # Get video duration
            video_duration = self._get_video_duration_from_metadata()

            # Determine number of frames if available
            frames_folder = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "frames"
            )
            num_frames = 0
            if os.path.exists(frames_folder):
                frames = [f for f in os.listdir(frames_folder)
                          if os.path.isfile(os.path.join(frames_folder, f))
                          and f.endswith(('.jpg', '.png'))]
                num_frames = len(frames)

            # Adaptive MIN_SCENE_DURATION: shorter for shorter videos
            if video_duration < 60:  # <1 minute
                self.MIN_SCENE_DURATION = 2.0
            elif video_duration < 300:  # 1-5 minutes
                self.MIN_SCENE_DURATION = 5.0
            else:  # >5 minutes
                self.MIN_SCENE_DURATION = 10.0

            # Adaptive MIN_TEXT_LENGTH: stricter for longer videos
            if video_duration < 60:
                self.MIN_TEXT_LENGTH = 30
            elif video_duration < 300:
                self.MIN_TEXT_LENGTH = 50
            else:
                self.MIN_TEXT_LENGTH = 80

            # Adaptive DEFAULT_SUMMARY_LENGTH: longer for longer videos
            if video_duration < 60:
                self.DEFAULT_SUMMARY_LENGTH = 100
            elif video_duration < 300:
                self.DEFAULT_SUMMARY_LENGTH = 130
            else:
                self.DEFAULT_SUMMARY_LENGTH = 180

            self.logger.info(f"Adaptive parameters set: MIN_SCENE_DURATION={self.MIN_SCENE_DURATION}, " +
                             f"MIN_TEXT_LENGTH={self.MIN_TEXT_LENGTH}, " +
                             f"DEFAULT_SUMMARY_LENGTH={self.DEFAULT_SUMMARY_LENGTH}")
        except Exception as e:
            self.logger.error(f"Error setting adaptive parameters: {str(e)}")
            # Keep default values if calculation fails
            self.logger.info("Using default parameters due to calculation error")

    def load_scene_data(self) -> List[Dict[str, Any]]:
        """
        Load and validate scene data from CSV file with robust error handling.
        Returns an empty list if no valid scenes are found.
        """
        scene_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            SCENE_SEGMENTED_FILE_CSV
        )

        if not os.path.exists(scene_file):
            self.logger.error(f"Scene file not found: {scene_file}")
            return []

        scenes = []
        try:
            with open(scene_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        scene = {
                            'start_time': float(row['start_time']),
                            'end_time': float(row['end_time']),
                            'description': row['description'].strip()
                        }

                        # Validate scene data
                        if (scene['end_time'] > scene['start_time'] and
                                len(scene['description']) >= self.MIN_TEXT_LENGTH):
                            scenes.append(scene)
                        else:
                            self.logger.warning(f"Skipping invalid scene: {scene}")

                    except (ValueError, KeyError) as e:
                        self.logger.error(f"Error processing scene row: {str(e)}")
                        continue

            return scenes

        except Exception as e:
            self.logger.error(f"Error loading scene data: {str(e)}")
            return []

    def create_fallback_scene(self) -> List[Dict[str, Any]]:
        """Create meaningful fallback scenes based on available captions when scene segmentation fails."""
        try:
            # Get video duration from metadata
            video_duration = self._get_video_duration_from_metadata()

            # Try to use rated captions from caption_score.csv
            caption_score_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "caption_score.csv"
            )

            if os.path.exists(caption_score_file):
                # Load captions with ratings
                rated_captions = []
                with open(caption_score_file, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            rated_captions.append({
                                'frame_index': int(row['frame_index']),
                                'caption': row['caption'],
                                'rating': float(row['rating'])
                            })
                        except (ValueError, KeyError) as e:
                            self.logger.warning(f"Skipping invalid caption row: {e}")
                            continue

                if rated_captions:
                    # Use enhanced quality filtering to select the best captions
                    quality_captions = self.select_quality_captions(rated_captions, video_duration)

                    if quality_captions:
                        # Determine appropriate number of scenes based on video duration
                        num_scenes = min(
                            len(quality_captions),
                            max(2, int(video_duration / 15))  # At least 2 scenes, approx 1 per 15s
                        )

                        # Create evenly spaced time segments
                        scene_duration = video_duration / num_scenes

                        # Create scenes with the best captions
                        scenes = []
                        for i in range(num_scenes):
                            start_time = i * scene_duration
                            end_time = min((i + 1) * scene_duration, video_duration)

                            # Use the appropriate caption for this time segment
                            caption_index = min(i, len(quality_captions) - 1)

                            scenes.append({
                                'start_time': start_time,
                                'end_time': end_time,
                                'text': quality_captions[caption_index]['caption']
                            })

                        # Add dialogue from transcript
                        self._integrate_dialogue_into_scenes(scenes)

                        return scenes

            # Fall back to regular captions.csv if caption_score.csv not available
            captions_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "captions.csv"
            )

            if os.path.exists(captions_file):
                captions = []
                with open(captions_file, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Handle different column naming conventions
                        caption_text = row.get('Caption', row.get('caption', ''))
                        if caption_text:
                            captions.append({
                                'caption': caption_text
                            })

                if captions:
                    # For short videos, create 4 scenes maximum
                    num_scenes = min(4, len(captions))
                    scene_duration = video_duration / num_scenes

                    # Select evenly distributed captions
                    indices = [int(i * len(captions) / num_scenes) for i in range(num_scenes)]
                    selected_captions = [captions[i] for i in indices]

                    # Create scenes
                    scenes = []
                    for i in range(num_scenes):
                        start_time = i * scene_duration
                        end_time = min((i + 1) * scene_duration, video_duration)

                        scenes.append({
                            'start_time': start_time,
                            'end_time': end_time,
                            'text': selected_captions[i]['caption']
                        })

                    # Add dialogue from transcript if available
                    self._integrate_dialogue_into_scenes(scenes)

                    return scenes

            # Last resort: Use metadata and watermarks
            description = self._create_description_from_metadata()

            # If we have a meaningful description, use it
            if description:
                return [{
                    'start_time': 0,
                    'end_time': video_duration,
                    'text': description
                }]

            # Absolute last resort - generic fallback
            return [{
                'start_time': 0,
                'end_time': video_duration,
                'text': "Complete video segment"
            }]

        except Exception as e:
            self.logger.error(f"Error creating fallback scene: {str(e)}")
            # Original generic fallback
            return [{
                'start_time': 0,
                'end_time': video_duration if video_duration else 60,
                'text': "Video segment"
            }]

    def _get_video_duration_from_metadata(self) -> float:
        """Get the video duration from metadata.json."""
        try:
            metadata_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "metadata.json"
            )
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return float(metadata.get("duration", 60))
            return 60.0  # Default if metadata file not found
        except Exception as e:
            self.logger.error(f"Error getting video duration: {str(e)}")
            return 60.0  # Default on error

    def _integrate_dialogue_into_scenes(self, scenes: List[Dict[str, Any]]) -> None:
        """Add dialogue from transcript to relevant scenes."""
        try:
            transcript_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "transcripts.json"
            )

            if not os.path.exists(transcript_file):
                return

            with open(transcript_file, 'r') as f:
                transcript_data = json.load(f)

            # Extract dialogue with timestamps
            for result in transcript_data.get('results', []):
                alternatives = result.get('alternatives', [])
                if alternatives and alternatives[0].get('transcript'):
                    words = alternatives[0].get('words', [])
                    if words:
                        # Get dialogue timing
                        start_time = float(words[0].get('start_time', 0))
                        end_time = float(words[-1].get('end_time', 0))
                        transcript_text = alternatives[0].get('transcript', '')

                        # Find matching scene and add dialogue
                        for scene in scenes:
                            scene_start = float(scene['start_time'])
                            scene_end = float(scene['end_time'])

                            if (start_time >= scene_start and start_time < scene_end) or \
                                    (end_time > scene_start and end_time <= scene_end):
                                scene['text'] = f"{scene['text']}. Dialogue: \"{transcript_text}\""
                                break
        except Exception as e:
            self.logger.error(f"Error integrating dialogue: {str(e)}")

    def _create_description_from_metadata(self) -> str:
        """Create a description using video metadata and watermarks."""
        try:
            # Get video title
            metadata_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "metadata.json"
            )
            video_title = ""

            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    video_title = metadata.get("title", "")

            # Get watermarks
            watermarks_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "count_vertice.json"
            )
            watermarks = []

            if os.path.exists(watermarks_file):
                with open(watermarks_file, 'r') as f:
                    watermarks_data = json.load(f)
                    watermarks = watermarks_data.get("watermarks", [])

            # Create description
            description = []

            if video_title:
                description.append(f"Video titled: {video_title}")

            if watermarks:
                description.append(f"Contains: {', '.join(watermarks)}")

            return ". ".join(description)
        except Exception as e:
            self.logger.error(f"Error creating description from metadata: {str(e)}")
            return ""

    def calculate_bleu_score(self, reference_sentences: List[str],
                             candidate_sentence: str) -> float:
        """
        Calculate BLEU score with improved error handling and smoothing.
        """
        try:
            if not reference_sentences or not candidate_sentence:
                return 0.0

            candidate = candidate_sentence.split()
            references = [ref.split() for ref in reference_sentences if ref.strip()]

            if not references or len(candidate) < 2:
                return 0.0

            method1 = SmoothingFunction().method1
            weights = (0.25, 0.25, 0.25, 0.25)

            return sentence_bleu(
                references,
                candidate,
                weights=weights,
                smoothing_function=method1
            )

        except Exception as e:
            self.logger.error(f"Error calculating BLEU score: {str(e)}")
            return 0.0

    def summarize_scene(self, scene: Dict[str, Any]) -> Optional[str]:
        """
        Summarize a single scene with error handling and validation.
        """
        try:
            if not scene.get('description'):
                return None

            # Clean and preprocess the text
            text = scene['description'].strip()
            if len(text) < self.MIN_TEXT_LENGTH:
                return text  # Return original if too short

            # Generate summary
            summary = self.summarizer(
                text,
                max_length=self.DEFAULT_SUMMARY_LENGTH,
                min_length=30,
                do_sample=False
            )

            if summary and summary[0]['summary_text']:
                return summary[0]['summary_text'].strip()
            return None

        except Exception as e:
            self.logger.error(f"Error summarizing scene: {str(e)}")
            return None

    @timeit
    def generate_text_summary(self) -> bool:
        """
        Main entry point for text summarization with comprehensive error handling.
        """
        try:
            self.logger.info("Starting text summarization")

            # Check if already processed
            if get_status_for_youtube_id(
                    self.video_runner_obj["video_id"],
                    self.video_runner_obj["AI_USER_ID"]
            ) == "done":
                self.logger.info("Text summarization already processed")
                return True

            # Load scene data
            scenes = self.load_scene_data()

            # Create fallback if no scenes found
            if not scenes:
                self.logger.warning("No valid scenes found, creating fallback")
                scenes = self.create_fallback_scene()

            # Process scenes
            summarized_scenes = []
            for scene in scenes:
                summary = self.summarize_scene(scene)
                if summary:
                    summarized_scenes.append({
                        'start_time': scene['start_time'],
                        'end_time': scene['end_time'],
                        'text': summary
                    })

            # Ensure we have at least one summary
            if not summarized_scenes:
                self.logger.warning("No summaries generated, using fallback")
                summarized_scenes = self.create_fallback_scene()

            # Save results
            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                SUMMARIZED_SCENES
            )

            with open(output_file, 'w') as f:
                json.dump(summarized_scenes, f, indent=2)

            self.logger.info(f"Text summarization completed: {len(summarized_scenes)} scenes")

            # Update database
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'text_summarization',
                {
                    "summarized_scenes": summarized_scenes,
                    "total_scenes": len(summarized_scenes)
                }
            )

            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )

            return True

        except Exception as e:
            self.logger.error(f"Error in text summarization: {str(e)}")
            self.logger.exception("Full traceback:")
            return False