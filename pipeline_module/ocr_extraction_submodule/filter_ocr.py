from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
import csv
import json
from ..utils_module.utils import return_video_folder_name, OCR_TEXT_CSV_FILE_NAME, OCR_FILTER_CSV_FILE_NAME, OCR_FILTER_REMOVE_SIMILAR
from ..utils_module.timeit_decorator import timeit

@timeit
def filter_ocr(video_runner_obj, window_width=10, threshold=0.5):

    incsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_CSV_FILE_NAME
    filtered_rows = []

    try:
        with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            header = next(reader)
            rows = [row for row in reader]
            blocks = [[]]
            current_block = blocks[0]

            for i in range(len(rows)):
                row = rows[i]
                text = json.loads(row[2])
                start = max(i - window_width, 0)
                best_rel_dist = 1.0
                best_comp_text = ""
                for j in range(start, i):
                    comp_text = json.loads(rows[j][2])
                    dist = levenshtein_dist(text[0]['description'], comp_text[0]['description'])
                    rel_dist = dist / max(len(text[0]['description']), len(comp_text[0]['description']))
                    if rel_dist < best_rel_dist:
                        best_rel_dist = rel_dist
                        best_comp_text = comp_text[0]['description']
                if best_rel_dist > threshold:
                    blocks.append([(row[0], row[1], text)])
                    current_block = blocks[-1]
                else:
                    current_block.append((row[0], row[1], text))

            for block in blocks:
                weights = []
                for (frame_index, timestamp, text) in block:
                    weight = 0.0
                    for (f_i, ts, comp_text) in block:
                        dist = levenshtein_dist(text[0]['description'], comp_text[0]['description'])
                        rel_dist = dist / max(len(text[0]['description']), len(comp_text[0]['description']))
                        weight += rel_dist
                    weights.append((frame_index, timestamp, text, weight))
                best_weight = float('inf')
                best_ocr = None
                for (frame_index, timestamp, text, weight) in weights:
                    if weight < best_weight:
                        best_weight = weight
                        best_ocr = [frame_index, timestamp, text]
                if best_ocr:
                    filtered_rows.append(best_ocr)

        outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_FILTER_CSV_FILE_NAME
        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            writer.writerow(header)
            for row in filtered_rows:
                writer.writerow([row[0], row[1], json.dumps(row[2])])
                outcsvfile.flush()

        # Save filtered OCR results to the database
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'filter_ocr', {"filtered_ocr": filtered_rows})

        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
        video_runner_obj["logger"].info("OCR filtering completed.")
        return True

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR filtering: {str(e)}")
        return False

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

    try:
        with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            header = next(reader)
            rows = [row for row in reader]
            for i in range(len(rows)):
                row = rows[i]
                text = json.loads(row[2])
                keep = True
                for kept_row in kept_rows:
                    kept_text = json.loads(kept_row[2])
                    diff = text_difference(text[0]['description'], kept_text[0]['description'])
                    if diff < threshold:
                        keep = False
                        break
                if keep:
                    kept_rows.append(row)

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

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR similarity removal: {str(e)}")
        return False

def text_difference(source, target):
    """
    Returns the Levenshtein distance between the source and target strings divided by the maximum of their lengths
    """
    maxlen = max(len(source), len(target))
    return levenshtein_dist(source, target) / maxlen if maxlen > 0 else 0