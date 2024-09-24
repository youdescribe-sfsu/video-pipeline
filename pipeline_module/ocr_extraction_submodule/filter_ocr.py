from web_server_module.web_server_database import get_status_for_youtube_id, update_status
import csv
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
            text = row[2]
            start = max(i - window_width, 0)
            best_rel_dist = 1.0
            best_comp_text = ""
            for j in range(start, i):
                comp_text = rows[j][2]
                dist = levenshtein_dist(text, comp_text)
                rel_dist = dist / max(len(text), len(comp_text))
                if rel_dist < best_rel_dist:
                    best_rel_dist = rel_dist
                    best_comp_text = comp_text
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
                    dist = levenshtein_dist(text, comp_text)
                    rel_dist = dist / max(len(text), len(comp_text))
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
            writer.writerow(row)
            outcsvfile.flush()

    # Save filtered OCR results to the database
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'filter_ocr', {"filtered_ocr": filtered_rows})

    update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
    video_runner_obj["logger"].info("OCR filtering completed.")
