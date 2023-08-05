# Use Andrew's YOLO object detection service to

import requests
import os
import csv
from utils import load_progress_from_file, return_video_frames_folder,return_video_folder_name,OBJECTS_CSV, save_progress_to_file
from timeit_decorator import timeit

YOLOv3_tiny = 8080
YOLOv3_Openimages = 8083
YOLOv3_9000 = 8084

import json

def get_object_from_YOLO(filename, threshold, service=YOLOv3_tiny,logger=None):
    """
    Use remote image object detection service provided by Andrew
    """
    # page = 
    token = os.getenv('ANDREW_YOLO_TOKEN')
    
    fileBuffer = open(filename, 'rb')
    multipart_form_data = {
        'token': ('', str(token)),
        'threshold': ('', str(threshold)),
        'img_file': (os.path.basename(filename), fileBuffer)
    }
    headers = {'token':token}
    page='http://localhost:{}/upload'.format(os.getenv('YOLO_PORT') or 8081)

    print('\n=====================')
    print("page",page)
    print("page ==",page)
    print(headers)
    print(multipart_form_data)
    print('=====================')
    if logger:
        logger.info(f"Running object detection for {filename}")
        logger.info(f"page: {page}")
        logger.info(f"headers: {headers}")
        logger.info(f"multipart_form_data: {multipart_form_data}")
        logger.info(f"=========================")
    
    try:
        response = requests.post(page, files=multipart_form_data)
        fileBuffer.close()
        print("=====in Object Detection=====")
        if logger:
            logger.info("========================")
            logger.info(f"response: {response.text}")
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            if logger:
                logger.info(f"Server returned status {response.status_code}")
            return []
        print(response.text)

        # Changes made here
        results = eval(response.text)
        response.close()
        return results

    except:
        response = requests.post(page, files=multipart_form_data)
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            logger.info(f"Server returned status {response.status_code}")
            return []
        logger.info(f"response: {response.text}")
        print(response.text)

        # Changes made here
        results = eval(response.text)
        response.close()
        return results


def detect_objects(video_files_path, threshold,video_runner_obj, service=YOLOv3_tiny, logging=False, logger=None):
    """
    Detects objects in each frame and collates the results into a dictionary
    The key is the name of the object, and each entry contains the frame index, detection confidence, and count
    """
    objects = {}
    # with open('{}/data.txt'.format(video_files_path), 'r') as datafile:
    #     data = datafile.readline().split()
    #     step = int(data[0])
    #     num_frames = int(data[1])
    last_processed_frame = 0
    save_data = load_progress_from_file(video_runner_obj=video_runner_obj)
    if(save_data['ObjectDetection']['started'] == False):
        save_data['ObjectDetection']['started'] = True
        save_data['ObjectDetection']['last_processed_frame'] = last_processed_frame
        save_data['ObjectDetection']['num_frames'] = save_data['OCR']['num_frames']
        save_data['ObjectDetection']['step'] = save_data['OCR']['num_frames']
        step = save_data['ObjectDetection']['step']
        num_frames = save_data['ObjectDetection']['num_frames']
        save_progress_to_file(video_runner_obj=video_runner_obj, progress_data=save_data)
    else:
        last_processed_frame = save_data['ObjectDetection']['last_processed_frame']
        num_frames = save_data['ObjectDetection']['num_frames']
        step = save_data['ObjectDetection']['step']
    

    for frame_index in range(last_processed_frame, num_frames, step):
        frame_filename = '{}/frame_{}.jpg'.format(video_files_path, frame_index)
        obj_list = get_object_from_YOLO(frame_filename, threshold, service, logger=logger)
        breakFor = True
        frame_objects = {}
        for entry in obj_list:
            name, prob, (x, y, w, h) = entry
            if name not in frame_objects:
                # NOTE (Lothar): Detections are sorted according to probability, so each object records the maximum confidence detection each frame
                frame_objects[name] = [frame_index, prob, 1]
            else:  # TODO (Lothar): Check bounds
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
        save_data['ObjectDetection']['last_processed_frame'] = last_processed_frame
        save_progress_to_file(video_runner_obj=video_runner_obj, progress_data=save_data)

    if logging:
        print('\rOn frame {}/{} (100% complete)          '.format(frame_index, num_frames))
    if logger:
        logger.info(f"Frame Index: {frame_index}")
    return objects


@timeit
def object_detection_to_csv(video_runner_obj):
    """
    Collates all detected objects into columns and tracks them from frame to frame
    """
    # get_object_from_YOLO(filename = '/home/datasets/pipeline/_THXHcNI82Y_files/frames/frame_1010.jpg',threshold = 0.00001)
    video_frames_path = return_video_frames_folder(video_runner_obj)
    video_runner_obj["logger"].info(f"Running object detection for {video_runner_obj['video_id']}")
    print("FILENAME "+video_frames_path)
    
    progress_file = load_progress_from_file(video_runner_obj=video_runner_obj)
    
    
    outcsvpath = return_video_folder_name(video_runner_obj)+ "/" + OBJECTS_CSV
    if not os.path.exists(outcsvpath):
        objects = detect_objects(video_frames_path, 0.001, logging=True,logger=video_runner_obj["logger"])
        video_runner_obj["logger"].info(f"Writing object detection results to {outcsvpath}")
        print(video_frames_path)
        with open('{}/data.txt'.format(video_frames_path), 'r') as datafile:
            data = datafile.readline().split()
            step = int(data[0])
            num_frames = int(data[1])

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


if __name__ == "__main__":
    # video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
    # video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
    # video_name = 'Good Samaritans knew that this puppy needed extra help'
    # video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
    # video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
    # video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
    video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'

    object_detection_to_csv(video_name)
