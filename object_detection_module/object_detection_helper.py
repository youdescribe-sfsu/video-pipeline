# Use Andrew's YOLO object detection service to

import requests
import os
import csv
from utils import returnVideoFramesFolder,returnVideoFolderName,OBJECTS_CSV
from dotenv import load_dotenv
from timeit_decorator import timeit

YOLOv3_tiny = 8080
YOLOv3_Openimages = 8083
YOLOv3_9000 = 8084


def get_object_from_YOLO(filename, threshold, service=YOLOv3_tiny,page='http://localhost:8082/upload'):
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
    try:
        response = requests.post(page, files=multipart_form_data)
        print("=====in Object Detection=====")
        print(response)
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            fileBuffer.close()
            return []
        print(response.text)

        # Changes made here
        results = eval(response.text)
        response.close()
        fileBuffer.close()
        return results

    except:
        response = requests.post(page, files=multipart_form_data)
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            fileBuffer.close()
            return []

        print(response.text)

        # Changes made here
        results = eval(response.text)
        response.close()
        fileBuffer.close()
        return results


def detect_objects(video_files_path, threshold, service=YOLOv3_tiny, logging=False,page='http://localhost:8082/upload'):
    """
    Detects objects in each frame and collates the results into a dictionary
    The key is the name of the object, and each entry contains the frame index, detection confidence, and count
    """
    objects = {}
    with open('{}/data.txt'.format(video_files_path), 'r') as datafile:
        data = datafile.readline().split()
        step = int(data[0])
        num_frames = int(data[1])

    for frame_index in range(0, num_frames, step):
        frame_filename = '{}/frame_{}.jpg'.format(video_files_path, frame_index)
        obj_list = get_object_from_YOLO(frame_filename, threshold, service,page)
        frame_objects = {}
        for entry in obj_list:
            name, prob, (x, y, w, h) = entry
            if name not in frame_objects:
                # NOTE(Lothar): Detections are sorted according to probability, so each object records the maximum confidence detection each frame
                frame_objects[name] = [frame_index, prob, 1]
            else:  # TODO(Lothar): Check bounds
                frame_objects[name][2] += 1
        for name, data in frame_objects.items():
            if name not in objects:
                objects[name] = []
            objects[name].append(data)
        if logging:
            print('\rOn frame {}/{} ({}% complete)          '.format(frame_index,
                  num_frames, (frame_index*100)//num_frames), end='')
    if logging:
        print('\rOn frame {}/{} (100% complete)          '.format(frame_index, num_frames))
    return objects

@timeit
def object_detection_to_csv(video_id,page='http://localhost:8082/upload'):
    """
    Collates all detected objects into columns and tracks them from frame to frame
    """
    video_frames_path = returnVideoFramesFolder(video_id)
    print("FILENAME "+video_frames_path)
    outcsvpath = returnVideoFolderName(video_id)+ "/" + OBJECTS_CSV
    if not os.path.exists(outcsvpath):
        objects = detect_objects(video_frames_path, 0.01, logging=True,page=page)
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
