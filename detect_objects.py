# Use Andrew's YOLO object detection service to

import requests
import os
import time
import csv

from dotenv import load_dotenv

YOLOv3_tiny = 8080
YOLOv3_Openimages = 8083
YOLOv3_9000 = 8084


def detect_objects(filename, threshold, service=YOLOv3_tiny):
    """
    Use remote image object detection service provided by Andrew
    """
    page = os.getenv('ANDREW_YOLO_UPLOAD_URL') + '/upload'
    token = os.getenv('ANDREW_YOLO_TOKEN')

    multipart_form_data = {
        'token': ('', str(token)),
        'threshold': ('', str(threshold)),
        'img_file': (os.path.basename(filename), open(filename, 'rb')),
    }
    print(multipart_form_data)
    try:
        response = requests.post(page, files=multipart_form_data)
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            return []
        print(response.text)
        return eval(response.text)

    except:
        response = requests.post(page, files=multipart_form_data)
        if response.status_code != 200:
            print("Server returned status {}.".format(response.status_code))
            return []

        print(response.text)
        return eval(response.text)


def track_objects(video_name, threshold, service=YOLOv3_tiny, logging=False):
    """
    Detects objects in each frame and collates the results into a dictionary
    The key is the name of the object, and each entry contains the frame index, detection confidence, and count
    """
    objects = {}
    video_name = video_name.split('/')[-1].split('.')[0]
    with open('{}/data.txt'.format(video_name), 'r') as datafile:
        data = datafile.readline().split()
        step = int(data[0])
        num_frames = int(data[1])

    for frame_index in range(0, num_frames, step):
        frame_filename = '{}/frame_{}.jpg'.format(video_name, frame_index)
        obj_list = detect_objects(frame_filename, threshold, service)
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


def object_tracking_to_csv(video_name):
    """
    Collates all detected objects into columns and tracks them from frame to frame
    """
    video_name = video_name.split('/')[-1].split('.')[0]
    outcsvpath = "Objects.csv"
    if not os.path.exists(outcsvpath):
        objects = track_objects(video_name, 0.1, logging=True)

        with open('{}/data.txt'.format(video_name), 'r') as datafile:
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

    object_tracking_to_csv(video_name)
