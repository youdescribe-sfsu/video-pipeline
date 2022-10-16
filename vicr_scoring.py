import csv
import sys 
from utils import OUTPUT_AVG_CSV, VICR_CSV, returnVideoFolderName, returnVideoFramesFolder

def keyframe_csv_to_json(video_id):
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

#def get_vicr_score_from_service(video_id):
    


if __name__ == '__main__':
    keyframe_csv_to_json(sys.argv[1])