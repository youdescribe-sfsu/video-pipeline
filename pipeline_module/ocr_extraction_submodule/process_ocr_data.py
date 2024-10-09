from web_server_module.web_server_database import update_module_output
from ..utils_module.timeit_decorator import timeit


@timeit
def process_ocr_data(video_runner_obj, ocr_annotations):
    logger = video_runner_obj.get("logger")

    # Step 1: Detect and remove watermarks
    ocr_data_without_watermarks = remove_watermarks(ocr_annotations, logger)

    # Step 2: Filter and remove duplicates
    filtered_ocr_data = filter_and_deduplicate(ocr_data_without_watermarks, logger)

    # Save processed data to the database
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"],
                         'process_ocr_data', {"filtered_ocr_data": filtered_ocr_data})

    return filtered_ocr_data


def remove_watermarks(ocr_annotations, logger):
    logger.info("Removing watermarks from OCR data")
    watermark_threshold = 0.8  # 80% occurrence to be considered a watermark
    total_frames = len(ocr_annotations)
    text_count = {}

    # Count occurrences of each text
    for frame_index, timestamp, texts in ocr_annotations:
        for text in texts:
            description = text['description']
            if description in text_count:
                text_count[description] += 1
            else:
                text_count[description] = 1

    # Identify watermarks
    watermarks = set(text for text, count in text_count.items() if count / total_frames >= watermark_threshold)

    # Remove watermarks from annotations
    clean_annotations = []
    for frame_index, timestamp, texts in ocr_annotations:
        clean_texts = [text for text in texts if text['description'] not in watermarks]
        if clean_texts:
            clean_annotations.append([frame_index, timestamp, clean_texts])

    return clean_annotations


def filter_and_deduplicate(ocr_data, logger):
    logger.info("Filtering and deduplicating OCR data")
    filtered_data = []
    previous_texts = set()

    for frame_index, timestamp, texts in ocr_data:
        unique_texts = []
        for text in texts:
            description = text['description']
            if description not in previous_texts and not is_similar(description, previous_texts):
                unique_texts.append(text)
                previous_texts.add(description)

        if unique_texts:
            filtered_data.append([frame_index, timestamp, unique_texts])

    return filtered_data


def is_similar(text, previous_texts, threshold=0.8):
    for prev_text in previous_texts:
        if text_similarity(text, prev_text) > threshold:
            return True
    return False


def text_similarity(text1, text2):
    return 1 - levenshtein_dist(text1, text2) / max(len(text1), len(text2))


def levenshtein_dist(s1, s2):
    if len(s1) < len(s2):
        return levenshtein_dist(s2, s1)
    if len(s2) == 0:
        return len(s1)
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]