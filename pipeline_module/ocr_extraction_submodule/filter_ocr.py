import csv
import json
from ..utils_module.utils import return_video_folder_name, OCR_FILTER_CSV_FILE_NAME, OCR_FILTER_REMOVE_SIMILAR, OCR_HEADERS, FRAME_INDEX_SELECTOR, TIMESTAMP_SELECTOR, OCR_TEXT_SELECTOR
from ..utils_module.timeit_decorator import timeit
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output


def text_difference(source, target):
    """
    Returns the Levenshtein distance between the source and target strings divided by the maximum of their lengths
    """
    maxlen = max(len(source), len(target))
    return levenshtein_dist(source, target) / maxlen if maxlen > 0 else 0


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


@timeit
def filter_ocr_remove_similarity(video_runner_obj, threshold=0.15, max_similar_lines=3):
    """
    Removes non-ASCII characters from all chosen texts and also removes any line
    of text after it has occurred max_similar_lines times
    """
    if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
        video_runner_obj["logger"].info("OCR similarity removal already completed, skipping step.")
        return True

    incsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_FILTER_CSV_FILE_NAME
    kept_rows = []

    with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
        reader = csv.reader(incsvfile)
        header = next(reader)
        rows = [row for row in reader]
        for i in range(len(rows)):
            row = rows[i]
            text = json.loads(row[OCR_HEADERS[OCR_TEXT_SELECTOR]])
            keep = True
            for kept_row in kept_rows:
                kept_text = json.loads(kept_row[OCR_HEADERS[OCR_TEXT_SELECTOR]])
                diff = text_difference(text[0]['description'], kept_text[0]['description'])
                if diff < threshold:
                    keep = False
                    break
            if keep:
                kept_rows.append(row)

    # Writing the filtered rows to output CSV file
    outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_FILTER_REMOVE_SIMILAR
    with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
        writer = csv.writer(outcsvfile)
        writer.writerow(header)
        for row in kept_rows:
            writer.writerow(row)
            outcsvfile.flush()

    # Update the database with filtered OCR results
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'filter_ocr_remove_similarity', {"filtered_ocr_remove_similar": kept_rows})
    update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
    video_runner_obj["logger"].info("OCR similarity removal completed.")

    return True


if __name__ == "__main__":
    # Example video_runner_obj for testing
    video_runner_obj = {
        "video_id": "example_id",
        "AI_USER_ID": "example_user",
        "logger": None  # Replace with actual logger instance
    }
    filter_ocr_remove_similarity(video_runner_obj)