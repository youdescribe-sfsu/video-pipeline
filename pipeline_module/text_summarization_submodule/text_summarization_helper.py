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
        # Define minimum scene length and text requirements
        self.MIN_SCENE_DURATION = 5.0  # seconds
        self.MIN_TEXT_LENGTH = 50  # characters
        self.DEFAULT_SUMMARY_LENGTH = 130

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
        """Create fallback scenes based on available captions."""
        try:
            # Get video duration from metadata
            video_duration = self._get_video_duration_from_metadata()

            # Try to use captions if available
            captions_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "captions.csv"
            )

            if os.path.exists(captions_file):
                with open(captions_file, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    captions = list(reader)

                # If we have captions, create time-based scenes with them
                if captions:
                    scenes = []

                    # For short videos, create 3-5 scenes maximum
                    num_scenes = min(4, len(captions))
                    scene_duration = video_duration / num_scenes

                    # Select highest-rated captions
                    selected_captions = captions[:num_scenes]

                    # Create scenes
                    for i, caption in enumerate(selected_captions):
                        start_time = i * scene_duration
                        end_time = min((i + 1) * scene_duration, video_duration)
                        scenes.append({
                            'start_time': start_time,
                            'end_time': end_time,
                            'text': caption['Caption']
                        })
                    return scenes

            # Only use the generic fallback if no captions available
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
                'end_time': 60,
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
                    return float(metadata.get("duration", 60))  # Default to 60 seconds if not found
            return 60.0  # Default duration if metadata file not found
        except Exception as e:
            self.logger.error(f"Error getting video duration: {str(e)}")
            return 60.0  # Default duration on error

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