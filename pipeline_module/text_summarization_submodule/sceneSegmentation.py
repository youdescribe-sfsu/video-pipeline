import csv
from ..utils_module.utils import OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, return_video_folder_name
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output

columns = {
    "start_time": "start_time",
    "end_time": "end_time",
    "description": "description",
}


def average_check(averageone, averagetwo, threshold):
    return averageone < threshold and averagetwo < threshold


def segmented_data(scene_time_limit, threshold, list_new):
    scenesegments = []
    current_scene_timestamp = 0
    first_skip = False
    skiptimestamp = None
    description = ""
    data = []

    for i in range(len(list_new)):
        if list_new[i][7] == 'True':
            description += "\n" + list_new[i][8]

        if list_new[i][4] != 'SKIP' and list_new[i][4] < threshold:
            if average_check(list_new[i][5], list_new[i][6], threshold) and list_new[i][
                1] - current_scene_timestamp > scene_time_limit:
                scenesegments.append(list_new[i][1])
                data.append([current_scene_timestamp, list_new[i][1], description])
                description = ""
                current_scene_timestamp = list_new[i][1]

        if list_new[i][4] != 'SKIP' and first_skip:
            if list_new[i][1] - skiptimestamp >= scene_time_limit:
                scenesegments.append(list_new[i][1])
                data.append([current_scene_timestamp, list_new[i][1], description])
                description = ""
                current_scene_timestamp = list_new[i][1]
            first_skip = False

        if list_new[i][4] == 'SKIP':
            if not first_skip:
                skiptimestamp = list_new[i][1]
                first_skip = True

    return data


def parse_csv_file(csv_path):
    list_new = []
    with open(csv_path, 'r') as csvFile:
        reader = csv.reader(csvFile)
        headers = next(reader)
        for row in reader:
            temp = []
            for idx, value in enumerate(row):
                if value == "":
                    temp.append(0.0)
                elif idx == 4 and value == "SKIP":
                    temp.append(value)
                elif idx == 7 or idx == 8:
                    temp.append(value)
                else:
                    temp.append(float(value))
            list_new.append(temp)
    return list_new


def scene_segmentation(video_runner_obj):
    """Segment the video into scenes based on frame similarities."""

    video_id = video_runner_obj["video_id"]
    logger = video_runner_obj.get("logger")

    # Check if process is already done
    if get_status_for_youtube_id(video_id, video_runner_obj["AI_USER_ID"]) == "done":
        logger.info(f"Scene segmentation already processed for video: {video_id}")
        return True

    output_avg_file = return_video_folder_name(video_runner_obj) + '/' + OUTPUT_AVG_CSV
    scene_segmented_file = return_video_folder_name(video_runner_obj) + '/' + SCENE_SEGMENTED_FILE_CSV

    list_new = parse_csv_file(output_avg_file)
    data = segmented_data(10, 0.75, list_new)

    with open(scene_segmented_file, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(columns.values())
        writer.writerows(data)

    logger.info(f"Scene segmentation results saved for video: {video_id}")

    # Save scenes to database
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'scene_segmentation',
                         {"scenes": data})

    # Mark process as done
    update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")

    return True