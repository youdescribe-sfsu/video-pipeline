import os
import subprocess
import csv
import json
from utils import OUTPUT_AVG_CSV, VICR_CSV, returnVideoFolderName, returnVideoFramesFolder
import requests
import argparse


def keyframe_csv_to_json(video_id):
    '''Convert keyframe csv to json'''
    csv_path = returnVideoFolderName(video_id) + '/' + OUTPUT_AVG_CSV
    print(csv_path)
    with open(csv_path, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 
        vicr_json = []
        #convert each csv row into python dict
        for row in csvReader:
            if(row['iskeyFrame'] == 'True'):
                vicr_json.append({
                    'description': row['description'],
                    'frame': returnVideoFramesFolder(video_id) + "/frame_" + row['frame'] + '.jpg'
                })
        vicr_json_to_csv(video_id,vicr_json)

def vicr_json_to_csv(video_id,vicr_json):
    '''Convert json to csv for VICR service'''
    csv_path = returnVideoFolderName(video_id) + '/' + VICR_CSV
    with open(csv_path, 'w', encoding='us-ascii') as csvf:
        writer = csv.writer(csvf)
        # Added Space after comma to make it readable
        writer.writerow(['image','caption'])
        for row in vicr_json:
            # Added Space after comma to make it readable
            writer.writerow([row['frame']," "+row['description']])
    print('VICR CSV created')
    print(returnVideoFolderName(video_id) + '/' + VICR_CSV)

def get_vicr_score_from_service(video_id):
    '''Get VICR score from service'''
    keyframe_csv_to_json(video_id)
    script = 'chmod -R 777 {}'.format(returnVideoFolderName(video_id))
    print(script)
    subprocess.run(script, shell=True, check=True)
    headers = {"Content-Type": "application/json; charset=utf-8"}
    print('==========')
    print(returnVideoFolderName(video_id) + '/' + VICR_CSV)
    print(video_id)
    print(int(os.environ['START_TIME']))
    print(int(os.environ['END_TIME']))
    print('==========')
    url = "http://localhost:7000/getvicrscore"

    payload = json.dumps({
    "video_id": video_id,
    "csv_file": returnVideoFolderName(video_id) + '/' + VICR_CSV,
    "start": int(os.environ['START_TIME']),
    "end": int(os.environ['END_TIME'])
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    print(response)
    print(response.text)
    # response = requests.request(method='get',url="http://localhost:7000/getvicrscore", , headers=headers)
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--yolo",default=8081, help="Yolo Port", type=int)
    parser.add_argument("--videoid", help="Video Id", type=str)
    parser.add_argument("--start_time",default=None, help="Start Time", type=str)
    parser.add_argument("--end_time",default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.videoid
    pagePort = args.yolo
    video_start_time =  args.start_time
    video_end_time = args.end_time
    os.environ['START_TIME'] = video_start_time
    os.environ['END_TIME'] = video_end_time
    get_vicr_score_from_service(video_id)