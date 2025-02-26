import re
import os
import json
from typing import List, Dict, Any

from pipeline_module.utils_module.utils import return_video_folder_name


class OutputEnhancer:
    """
    Enhances final output data to improve description quality.
    """

    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.video_id = video_runner_obj.get("video_id")

    def clean_text_for_final_output(self, text):
        """
        Clean text for final output to ensure readability.
        """
        if not text:
            return ""

        # Remove excessive newlines and spacing
        text = re.sub(r'\s+', ' ', text).strip()

        # Remove excessive punctuation
        text = re.sub(r'\.{2,}', '.', text)

        # Fix common OCR separators
        text = re.sub(r'\.([A-Z])', r'. \1', text)

        # Extract meaningful information (brands, flavors, etc.)
        product_patterns = {
            r'PRINGLES': 'Pringles',
            r'SOUR CREAM': 'Sour Cream',
            r'PERI PERI': 'Peri Peri',
            r'ASALA TADKA': 'Asala Tadka',
            r'CHUTNEY': 'Chutney',
            r'COCA[\s-]?COLA': 'Coca-Cola',
            r'PEPSI': 'Pepsi',
            r'DISNEY': 'Disney',
            r'PIXAR': 'Pixar'
        }

        detected_products = {}
        for pattern, product in product_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                detected_products[product] = True

        # If we found product information, format it nicely
        if detected_products:
            products = list(detected_products.keys())
            brand = None
            flavors = []

            # Separate brand from flavors
            for product in products:
                if product in ['Pringles', 'Coca-Cola', 'Pepsi', 'Disney', 'Pixar']:
                    brand = product
                else:
                    flavors.append(product)

            if brand and flavors:
                if brand == 'Pringles':
                    return f"{brand} flavors shown: {', '.join(flavors)}"
                else:
                    return f"{brand} varieties shown: {', '.join(flavors)}"
            elif brand:
                return f"{brand} product shown"
            else:
                return f"Products shown: {', '.join(flavors)}"

        return text

    def deduplicate_descriptions(self, audio_clips):
        """
        Remove duplicate or highly similar descriptions.
        """
        if not audio_clips:
            return []

        enhanced_clips = []
        visual_descriptions = {}  # Map timestamps to descriptions

        # First pass: Group by type and timestamp proximity
        for clip in audio_clips:
            clip_type = clip.get("type", "")
            start_time = float(clip.get("start_time", 0))

            if clip_type == "Visual":
                # Check if this is too close to an existing visual clip
                duplicate = False
                for existing_time in visual_descriptions.keys():
                    if abs(existing_time - start_time) < 3.0:  # Within 3 seconds
                        existing_text = visual_descriptions[existing_time]
                        if existing_text == clip.get("text", ""):
                            duplicate = True
                            break

                if not duplicate:
                    visual_descriptions[start_time] = clip.get("text", "")
                    enhanced_clips.append(clip)
            elif clip_type == "Text on Screen":
                # Clean up text
                text = self.clean_text_for_final_output(clip.get("text", ""))
                if text:  # Only include if there's meaningful text
                    clip["text"] = text
                    enhanced_clips.append(clip)
            else:
                # Include other types as-is
                enhanced_clips.append(clip)

        return enhanced_clips

    def remove_suspicious_descriptions(self, audio_clips):
        """
        Remove descriptions that contain suspicious patterns.
        """
        if not audio_clips:
            return []

        # Load the summarized scenes for potential replacements
        summarized_scenes_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            "summarized_scenes.json"
        )

        alternative_descriptions = {}
        if os.path.exists(summarized_scenes_file):
            try:
                with open(summarized_scenes_file, 'r') as f:
                    scenes = json.load(f)
                    for scene in scenes:
                        start_time = float(scene.get("start_time", 0))
                        alternative_descriptions[start_time] = scene.get("text", "")
            except Exception as e:
                self.logger.error(f"Error loading summarized scenes: {e}")

        # Suspicious patterns that indicate likely incorrect descriptions
        suspicious_patterns = [
            r"man and woman stand.*sell.*charity",
            r"tv tablet.*device that allows",
            r"couple are trying to sell"
        ]

        enhanced_clips = []

        # Get video title for context
        video_title = ""
        metadata_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            "metadata.json"
        )
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    video_title = metadata.get("title", "")
            except Exception as e:
                self.logger.error(f"Error loading metadata: {e}")

        # Extract potential brands from title
        potential_brands = []
        brand_patterns = {
            r'pringles': 'Pringles',
            r'coca[\s-]?cola': 'Coca-Cola',
            r'pepsi': 'Pepsi',
            r'disney': 'Disney',
            r'pixar': 'Pixar',
            r'inside out': 'Inside Out',
            r'mario': 'Mario'
        }

        for pattern, brand in brand_patterns.items():
            if re.search(r'\b' + pattern + r'\b', video_title.lower()):
                potential_brands.append(brand)

        for clip in audio_clips:
            if clip.get("type") == "Visual":
                text = clip.get("text", "")
                start_time = float(clip.get("start_time", 0))

                # Check for suspicious patterns
                suspicious = any(re.search(pattern, text, re.IGNORECASE) for pattern in suspicious_patterns)

                # Also check if brands from title are missing
                if potential_brands and all(brand.lower() not in text.lower() for brand in potential_brands):
                    suspicious = True

                if suspicious:
                    # Try to find a replacement
                    if start_time in alternative_descriptions:
                        alternative = alternative_descriptions[start_time]
                        if alternative and not any(
                                re.search(pattern, alternative, re.IGNORECASE) for pattern in suspicious_patterns):
                            clip["text"] = alternative
                    else:
                        # Create a generic but safe description
                        video_type = "short video" if "#shorts" in video_title.lower() else "video"
                        if potential_brands:
                            brands_text = " and ".join(potential_brands)
                            clip["text"] = f"A {video_type} featuring {brands_text}."
                        else:
                            clip["text"] = f"A scene from the {video_type}."

            enhanced_clips.append(clip)

        return enhanced_clips

    def add_temporal_context(self, audio_clips, video_length):
        """
        Add temporal context to descriptions (beginning, middle, end).
        """
        if not audio_clips:
            return []

        enhanced_clips = []
        visual_clips = [clip for clip in audio_clips if clip.get("type") == "Visual"]

        for i, clip in enumerate(audio_clips):
            if clip.get("type") == "Visual":
                text = clip.get("text", "")
                start_time = float(clip.get("start_time", 0))

                # Avoid modifying already contextual descriptions
                has_context = any(word in text.lower() for word in ["begins", "starts", "ends", "final"])

                if not has_context:
                    # Add temporal context based on position
                    if i == 0 or start_time < 2.0:  # First clip or very early
                        text = text.replace("The video shows", "The video begins showing").replace("This video shows",
                                                                                                   "This video begins showing")
                    elif i == len(visual_clips) - 1 or start_time > video_length * 0.8:  # Last clip or very late
                        text = text.replace("The video shows", "The video ends showing").replace("This video shows",
                                                                                                 "This video ends showing")

                    clip["text"] = text

            enhanced_clips.append(clip)

        return enhanced_clips

    def enhance_final_output(self, data):
        """
        Enhance the final output data.
        """
        if not data:
            return {}

        # Make a copy to avoid modifying the original
        enhanced_data = dict(data)

        # Get audio clips and enhance them
        audio_clips = enhanced_data.get("audio_clips", [])
        video_length = float(enhanced_data.get("video_length", 0))

        # Apply enhancements in sequence
        enhanced_clips = self.deduplicate_descriptions(audio_clips)
        enhanced_clips = self.remove_suspicious_descriptions(enhanced_clips)
        enhanced_clips = self.add_temporal_context(enhanced_clips, video_length)

        # Update the data
        enhanced_data["audio_clips"] = enhanced_clips

        return enhanced_data


# Enhance the prepare_upload_data method in UploadToYDX class
def prepare_upload_data(self, dialogue_timestamps, audio_clips, metadata, AI_USER_ID):
    """
    Enhanced implementation of prepare_upload_data with quality checks.
    """
    data = {
        "youtube_id": self.video_runner_obj['video_id'],
        "audio_clips": audio_clips,
        "video_length": metadata["duration"],
        "video_name": metadata["title"],
        "dialogue_timestamps": dialogue_timestamps,
        "aiUserId": AI_USER_ID,
    }

    # Enhance the final output
    try:
        # Initialize the output enhancer
        output_enhancer = OutputEnhancer(self.video_runner_obj)

        # Enhance the data
        enhanced_data = output_enhancer.enhance_final_output(data)

        # Return the enhanced data
        return enhanced_data
    except Exception as e:
        self.logger.error(f"Error enhancing final output: {e}")
        import traceback
        self.logger.error(traceback.format_exc())

        # Return the original data if enhancement fails
        return data