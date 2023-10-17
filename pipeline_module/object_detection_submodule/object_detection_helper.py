import requests
import os
import csv
import json
import traceback
from ..utils_module.utils import read_value_from_file, return_video_frames_folder, return_video_folder_name, OBJECTS_CSV, save_value_to_file
from ..utils_module.timeit_decorator import timeit

DEFAULT_OBJECT_DETECTION_BATCH_SIZE = 100
IMAGE_THRESHOLD = 0.25

def get_object_from_YOLO_batch(files_path, threshold, logger=None):
    """
    Use remote image object detection service provided by YOLO
    """
    token = os.getenv('ANDREW_YOLO_TOKEN')
    yolo_port = os.getenv('YOLO_PORT') or '8087'
        
    payload = json.dumps({
        "files_path": files_path,
        "threshold": threshold
    })
    
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
    }
    
    url = f'http://localhost:{yolo_port}/detect_multiple_files'
    
    print("url :: ",url)
    
    return_val = None

    if logger:
        logger.info(f"Running object detection for {str(files_path)}")
        logger.info(f"Running object detection on URL {url}")
        logger.info(f"=========================")
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            if logger:
                logger.info(f"Server returned status {response.status_code}")
            return_val = []

        response_data = eval(response.text)
        response.close()
        return_val = response_data['results']

    except requests.exceptions.Timeout:
        print("Request timed out")
        raise Exception('Request timed out')
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        raise Exception(f"Request error: {e}")
    return return_val

def get_object_from_YOLO(filename, threshold, logger=None):
    """
    Use remote image object detection service provided by YOLO
    """
    token = os.getenv('ANDREW_YOLO_TOKEN')
    yolo_port = os.getenv('YOLO_PORT') or '8087'
        
    payload = json.dumps({
        "filename": filename,
        "threshold": threshold
    })
    
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
    }
    
    url = f'http://localhost:{yolo_port}/detect_single_file'
    
    print("url :: ",url)
    
    return_val = None

    if logger:
        logger.info(f"Running object detection for {filename}")
        logger.info(f"Running object detection on URL {url}")
        logger.info(f"=========================")
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            if logger:
                logger.info(f"Server returned status {response.status_code}")
            return_val = []

        response_data = eval(response.text)
        response.close()
        return_val = response_data['results']

    except requests.exceptions.Timeout:
        print("Request timed out")
        raise Exception('Request timed out')
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        raise Exception(f"Request error: {e}")
    return return_val

def detect_objects(video_files_path, threshold, video_runner_obj, logging=False, logger=None):
    """
    Detects objects in each frame and collates the results into a dictionary
    The key is the name of the object, and each entry contains the frame index, detection confidence, and count
    """
    objects = {}
    last_processed_frame = 0

    if not read_value_from_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['started']"):
        save_value_to_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['started']", value=True)
        step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")
        num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
    else:
        last_processed_frame = read_value_from_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['last_processed_frame']")
        num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
        step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")

    for frame_index in range(last_processed_frame, num_frames, step):
        frame_filename = '{}/frame_{}.jpg'.format(video_files_path, frame_index)
        obj_list = get_object_from_YOLO(frame_filename, threshold, logger=logger)
        frame_objects = {}
        for entry in obj_list:
            name = entry['name']
            prob = entry['confidence']
            if name not in frame_objects:
                frame_objects[name] = [frame_index, prob, 1]
            else:
                frame_objects[name][2] += 1
        for name, data in frame_objects.items():
            if name not in objects:
                objects[name] = []
            objects[name].append(data)
        if logging:
            print('\rOn frame {}/{} ({}% complete)          '.format(frame_index, num_frames, (frame_index*100)//num_frames), end='')
        if logger:
            logger.info(f"Frame Index: {frame_index}")
        last_processed_frame = frame_index
        save_value_to_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['last_processed_frame']", value=last_processed_frame)

    if logging:
        print('\rOn frame {}/{} (100% complete)          '.format(frame_index, num_frames))
    if logger:
        logger.info(f"Frame Index: {frame_index}")
    return objects

def process_batch_response(batch_response, objects):
    for response in batch_response:
        frame_index = response['frame_number']
        obj_list = response['confidences']
        for entry in obj_list:
            name = entry['name']
            prob = entry['confidence']
            if name not in objects:
                objects[name] = []
            objects[name].append([frame_index, prob, 1])
    return objects

def detect_objects_batch(video_files_path, threshold, video_runner_obj, logging=False, logger=None):
    objects = {}
    last_processed_frame = 0

    if not read_value_from_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['started']"):
        save_value_to_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['started']", value=True)
        step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")
        num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
    else:
        last_processed_frame = read_value_from_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['last_processed_frame']")
        num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
        step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")

    batch_size = DEFAULT_OBJECT_DETECTION_BATCH_SIZE

    batched_frame_filenames = []

    for frame_index in range(last_processed_frame, num_frames, step):
        frame_filename = '{}/frame_{}.jpg'.format(video_files_path, frame_index)
        batched_frame_filenames.append(frame_filename)

        if len(batched_frame_filenames) == batch_size or frame_index == num_frames - 1:
            batch_response = get_object_from_YOLO_batch(batched_frame_filenames, threshold, logger=logger)
            objects = process_batch_response(batch_response=batch_response, objects=objects)

            if logging:
                print('\rOn frame {}/{} ({}% complete)          '.format(frame_index, num_frames, (frame_index * 100) // num_frames), end='')

            if logger:
                logger.info(f"Frame Index: {frame_index}")

            last_processed_frame = frame_index
            save_value_to_file(video_runner_obj=video_runner_obj, key="['ObjectDetection']['last_processed_frame']", value=last_processed_frame)

            batched_frame_filenames = []

    if len(batched_frame_filenames) > 0:
        batch_response = get_object_from_YOLO_batch(batched_frame_filenames, threshold, logger=logger)
        objects = process_batch_response(batch_response=batch_response, objects=objects)

    if logging:
        print('\rOn frame {}/{} (100% complete)          '.format(frame_index, num_frames))

    if logger:
        logger.info(f"Frame Index: {frame_index}")

    return objects

@timeit
def object_detection_to_csv(video_runner_obj):
    video_frames_path = return_video_frames_folder(video_runner_obj)
    video_runner_obj["logger"].info(f"Running object detection for {video_runner_obj['video_id']}")
    
    outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OBJECTS_CSV
    num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
    step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")

    if not os.path.exists(outcsvpath):
        try:
            objects = detect_objects_batch(video_frames_path, IMAGE_THRESHOLD, video_runner_obj=video_runner_obj, logging=True, logger=video_runner_obj["logger"])
            video_runner_obj["logger"].info(f"Writing object detection results to {outcsvpath}")
            video_runner_obj["logger"].info(f"video_frames_path: {video_frames_path}")

            with open(outcsvpath, 'a', newline='') as outcsvfile:
                writer = csv.writer(outcsvfile)
                header = ['frame_index']
                for name, data in objects.items():
                    header.append(name)
                    header.append('')
                writer.writerow(header)
                for frame_index in range(0, num_frames, step):
                    row = [frame_index]
                    for name, data in objects.items():
                        found = False
                        for entry in data:
                            if entry[0] == frame_index:
                                found = True
                                row.append(entry[1])
                                row.append(entry[2])
                                break
                            if entry[0] > frame_index:
                                break
                        if not found:
                            row.append('')
                            row.append('')
                    writer.writerow(row)
                    outcsvfile.flush()
            return True
        except Exception as e:
            traceback.print_exc()
            print(e)
            return False

if __name__ == "__main__":
    video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'
    object_detection_to_csv(video_name)
