import re
import json
import os
import traceback

from ..utils_module.utils import return_video_folder_name, COUNT_VERTICE

def detect_watermarks(video_runner_obj, ocr_annotations):
    """
    Automatically identify text elements that appear consistently across frames.
    These are likely watermarks, logos, or UI elements that should be filtered.

    Args:
        video_runner_obj: Dictionary containing video processing information
        ocr_annotations: List of OCR annotation results

    Returns:
        List of detected watermark text strings
    """
    logger = video_runner_obj.get("logger")

    try:
        watermarks = []
        text_count = {}
        total_frames = len(ocr_annotations)

        # Skip detection if too few frames
        if total_frames < 3:
            return watermarks

        # Count text occurrence across frames
        for ann in ocr_annotations:
            for text in ann["texts"]:
                description = text["description"].strip()
                if len(description) > 2:  # Ignore very short text
                    if description not in text_count:
                        text_count[description] = 0
                    text_count[description] += 1

        # Text appearing in more than 60% of frames is likely a watermark
        threshold = 0.6 * total_frames
        for text, count in text_count.items():
            if count > threshold:
                # Additional checks to avoid false positives
                # Very long texts are unlikely to be watermarks
                if len(text) < 50:
                    watermarks.append(text)

        # Save detected watermarks
        output_file = f"{return_video_folder_name(video_runner_obj)}/{COUNT_VERTICE}"
        with open(output_file, 'w', encoding='utf-8') as jsonfile:
            json.dump({"watermarks": watermarks}, jsonfile)

        logger.info(f"Detected {len(watermarks)} watermarks: {watermarks}")
        return watermarks

    except Exception as e:
        logger.error(f"Error detecting watermarks: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def remove_watermarks_and_filter(ocr_annotations, watermarks):
    """
    Remove watermarks from OCR data and perform additional filtering.

    Args:
        ocr_annotations: List of OCR annotation results
        watermarks: List of watermark text to remove

    Returns:
        Filtered OCR data with watermarks removed
    """
    filtered_data = []

    # Process each annotation
    for ann in ocr_annotations:
        filtered_texts = []

        for text in ann["texts"]:
            # Skip detected watermarks
            if any(watermark in text["description"] for watermark in watermarks):
                continue

            # Skip very short or empty text
            if len(text["description"].strip()) < 3:
                continue

            # Skip text that's likely noise (too many special characters)
            special_char_count = sum(1 for c in text["description"] if not c.isalnum() and not c.isspace())
            if special_char_count > len(text["description"]) * 0.5:
                continue

            filtered_texts.append(text)

        # Only include frames that still have text after filtering
        if filtered_texts:
            filtered_data.append({
                "frame_index": ann["frame_index"],
                "timestamp": ann["timestamp"],
                "texts": filtered_texts
            })

    return filtered_data


def remove_similar_entries(filtered_data):
    """
    Remove duplicate or highly similar OCR text entries.

    Args:
        filtered_data: List of filtered OCR data

    Returns:
        OCR data with similar entries removed
    """
    final_filtered_data = []
    seen_texts = set()

    # Process each frame
    for data in filtered_data:
        unique_texts = []

        for text in data["texts"]:
            text_content = text["description"].strip()

            # Skip if exactly seen before
            if text_content in seen_texts:
                continue

            # Check for high similarity with existing texts
            found_similar = False
            for seen_text in seen_texts:
                # Simple similarity: common prefix/suffix or small edit distance
                if (len(text_content) > 5 and (
                        text_content.startswith(seen_text[:5]) or
                        text_content.endswith(seen_text[-5:]) or
                        seen_text.startswith(text_content[:5]) or
                        seen_text.endswith(text_content[-5:])
                )):
                    found_similar = True
                    break

            if not found_similar:
                seen_texts.add(text_content)
                unique_texts.append(text)

        # Only include frames that still have text after filtering
        if unique_texts:
            final_filtered_data.append({
                "frame_index": data["frame_index"],
                "timestamp": data["timestamp"],
                "texts": unique_texts
            })

    return final_filtered_data

def clean_ocr_text(text, watermarks=None):
    """
    Clean OCR text by removing repetitions, fixing separators, and
    extracting meaningful content.

    Args:
        text (str): The raw OCR text
        watermarks (list): Optional list of watermark words to handle

    Returns:
        str: Cleaned and normalized OCR text
    """
    if not text or len(text) < 2:
        return ""

    # Use provided watermarks or load from file if not provided
    if watermarks is None:
        try:
            watermarks_file = os.path.join(return_video_folder_name(
                {"video_id": os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))}),
                                           COUNT_VERTICE)
            if os.path.exists(watermarks_file):
                with open(watermarks_file, 'r') as f:
                    watermarks_data = json.load(f)
                    watermarks = watermarks_data.get("watermarks", [])
        except Exception as e:
            print(f"Error loading watermarks: {e}")
            watermarks = []

    # Original text for fallback
    original_text = text

    # Remove excessive repetitions of watermarks
    for watermark in watermarks:
        # Replace multiple occurrences with single occurrence
        pattern = f"{re.escape(watermark)}(\\.[^.]*{re.escape(watermark)})*"
        text = re.sub(pattern, watermark, text, flags=re.IGNORECASE)

    # Replace separators with spaces
    text = re.sub(r'\.\s*([A-Z])', r' \1', text)

    # Remove unnecessary characters
    text = re.sub(r'[\n\r]+', ' ', text)

    # Normalize spaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Extract meaningful product information
    product_info = {}
    flavor_patterns = [
        r'\b(SOUR CREAM)\b',
        r'\b(PERI PERI)\b',
        r'\b(ASALA TADKA)\b',
        r'\b(CHUTNEY)\b',
        r'\b(SALT)\b',
        r'\b(ORIGINAL)\b',
        r'\b(BBQ)\b',
        r'\b(CHEESE)\b'
    ]

    for pattern in flavor_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            product_info[match.upper()] = True

    # Brand patterns for common products
    brand_patterns = [
        (r'\b(PRINGLES)\b', "Pringles"),
        (r'\b(DORITOS)\b', "Doritos"),
        (r'\b(COCA[\s-]?COLA)\b', "Coca-Cola"),
        (r'\b(PEPSI)\b', "Pepsi"),
        (r'\b(DISNEY)\b', "Disney"),
        (r'\b(PIXAR)\b', "Pixar"),
        (r'\b(INSIDE OUT)\b', "Inside Out"),
        (r'\b(MARIO)\b', "Mario"),
        (r'\b(IPHONE)\b', "iPhone"),
        (r'\b(SAMSUNG)\b', "Samsung"),
        (r'\b(ANDROID)\b', "Android"),
        (r'\b(NETFLIX)\b', "Netflix")
    ]

    detected_brands = set()
    for pattern, brand in brand_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            detected_brands.add(brand)

    # If we found meaningful product information, format it nicely
    if product_info or detected_brands:
        output_text = ""

        if detected_brands:
            brand_text = ", ".join(detected_brands)
            output_text += f"{brand_text} "

        if product_info:
            flavors = list(product_info.keys())
            if len(flavors) > 0:
                if "Pringles" in detected_brands:
                    output_text += f"flavors shown: {', '.join(flavors)}"
                else:
                    output_text += f"varieties shown: {', '.join(flavors)}"

        return output_text.strip()

    # If the cleaning process stripped too much, return original with basic cleanup
    if len(text) < 5 and len(original_text) > 10:
        # Minimal cleaning for the original
        return re.sub(r'[\n\r]+', ' ', original_text).strip()

    return text


# Modify the existing process_ocr_data function
def process_ocr_data(video_runner_obj, ocr_annotations):
    """
    Process OCR data with enhanced cleaning and filtering.
    This replaces or enhances the existing process_ocr_data function.
    """
    logger = video_runner_obj.get("logger")
    try:
        # Load watermarks
        watermarks_file = f"{return_video_folder_name(video_runner_obj)}/{COUNT_VERTICE}"
        watermarks = []

        if os.path.exists(watermarks_file):
            with open(watermarks_file, 'r') as f:
                watermarks_data = json.load(f)
                watermarks = watermarks_data.get("watermarks", [])

        # Generate OCR_TEXT_CSV_FILE_NAME with cleaned text
        generate_ocr_text_csv(video_runner_obj, ocr_annotations, watermarks)

        # Detect watermarks if not already present
        if not watermarks:
            watermarks = detect_watermarks(video_runner_obj, ocr_annotations)

        # Remove watermarks and filter
        filtered_data = remove_watermarks_and_filter(ocr_annotations, watermarks)

        # Generate OCR_FILTER_CSV_FILE_NAME with cleaned text
        generate_ocr_filter_csv(video_runner_obj, filtered_data, watermarks)

        # Remove similar entries and generate OCR_FILTER_REMOVE_SIMILAR
        final_filtered_data = remove_similar_entries(filtered_data)
        generate_ocr_filter_remove_similar(video_runner_obj, final_filtered_data, watermarks)

        # Update module output for tracking
        from web_server_module.web_server_database import update_module_output
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"],
                             'process_ocr_data', {"ocr_processing_complete": True})

        return final_filtered_data
    except Exception as e:
        logger.error(f"Error in enhanced OCR processing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        # Fall back to original implementation if enhancement fails
        # You can insert the original implementation here as a fallback

        return ocr_annotations


# Update these helper functions to use the OCR text cleaning
def generate_ocr_text_csv(video_runner_obj, ocr_annotations, watermarks=None):
    from ..utils_module.utils import return_video_folder_name, OCR_TEXT_CSV_FILE_NAME
    import csv

    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_TEXT_CSV_FILE_NAME}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
        for ann in ocr_annotations:
            for text in ann["texts"]:
                cleaned_text = clean_ocr_text(text["description"], watermarks)
                if cleaned_text:  # Only write non-empty text
                    writer.writerow([ann["frame_index"], ann["timestamp"], cleaned_text])


def generate_ocr_filter_csv(video_runner_obj, filtered_data, watermarks=None):
    from ..utils_module.utils import return_video_folder_name, OCR_FILTER_CSV_FILE_NAME
    import csv

    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_FILTER_CSV_FILE_NAME}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
        for data in filtered_data:
            for text in data["texts"]:
                cleaned_text = clean_ocr_text(text["description"], watermarks)
                if cleaned_text:  # Only write non-empty text
                    writer.writerow([data["frame_index"], data["timestamp"], cleaned_text])


def generate_ocr_filter_remove_similar(video_runner_obj, final_filtered_data, watermarks=None):
    from ..utils_module.utils import return_video_folder_name, OCR_FILTER_REMOVE_SIMILAR
    import csv

    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_FILTER_REMOVE_SIMILAR}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
        for data in final_filtered_data:
            for text in data["texts"]:
                cleaned_text = clean_ocr_text(text["description"], watermarks)
                if cleaned_text:  # Only write non-empty text
                    writer.writerow([data["frame_index"], data["timestamp"], cleaned_text])