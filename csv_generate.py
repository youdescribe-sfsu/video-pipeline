import csv
from utils import CAPTIONS_AND_OBJECTS_CSV, OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, returnVideoFolderName
from numpy import dot
from numpy.linalg import norm
import numpy as np
import warnings
warnings.filterwarnings("error")


columns = {
  "frameindex": "frame",
  "timestamp": "timestamp",
  "Line1": "Line1",
  "Line2": "Line2",
  "Sim": "Similarity",
  "Averageone": "avgone",
  "Averagetwo": "avgtwo",
  "iskeyFrame": "iskeyFrame",
  "description": "description",
}

def cosine_similarity(v1,v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(v1)):
        x = v1[i]; y = v2[i]
        sumxx += x*x
        sumyy += y*y
        sumxy += x*y
    try:
        return dot(v1, v2)/(norm(v1)*norm(v2))
    except RuntimeWarning:
        return "NaN"

def generateOutputAvg(video_id):
    '''Generate output avg csv file for a video'''
    captions_and_objects_csv = returnVideoFolderName(video_id)+'/'+CAPTIONS_AND_OBJECTS_CSV
    output_avg_csv = OUTPUT_AVG_CSV
    with open(captions_and_objects_csv, 'r') as csvFile:
        reader = csv.reader(csvFile)
        headers = next(reader)
        isKeyFrame = []
        description = []
        frame_index = []
        timestamp = []
        list = []
        for row in reader:
            temp = []
            isKeyFrame.append(row[2])
            description.append(row[3])
            frame_index.append(float(row[0]))
            timestamp.append(float(row[1]))
            for idx in range(4,len(headers)):
                if(row[idx] != ''):
                    temp.append(float(row[idx]))
                else:
                    temp.append(0.0)
            list.append(temp)
        data = []
        for idx in range(2,len(list) - 1):
            s = cosine_similarity(list[idx],list[idx+1])
            if(s == "NaN"):
                s = "SKIP"
            a1 = None
            a2 = None
            if(idx < len(list) - 3):
                a1 = cosine_similarity(list[idx-1],list[idx+2])
                a2 = cosine_similarity(list[idx-2],list[idx+3])
            else:
                a1 = 0.0
                a2 = 0.0
            data.append([frame_index[idx],timestamp[idx],idx,idx+1,s,a1,a2,isKeyFrame[idx],description[idx]])
        with open(output_avg_csv, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(['frame','timestamp','Line1','Line2','Similarity','avgone','avgtwo','iskeyFrame','description'])
            writer.writerows(data)
                
if __name__ == "__main__":
    generateOutputAvg('upSnt11tngE')