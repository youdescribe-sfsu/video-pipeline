from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
import csv
import json
from ..utils_module.utils import return_video_folder_name, OCR_TEXT_CSV_FILE_NAME, OCR_FILTER_CSV_FILE_NAME
from ..utils_module.timeit_decorator import timeit

@timeit
def filter_ocr(video_runner_obj, window_width=10, threshold=0.5):
    if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
        video_runner_obj["logger"].info("OCR filtering already completed, skipping step.")
        return True

    incsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_CSV_FILE_NAME
    filtered_rows = []

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