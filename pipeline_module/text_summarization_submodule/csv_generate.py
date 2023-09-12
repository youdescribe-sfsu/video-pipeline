import csv
from ..utils_module.utils import CAPTIONS_AND_OBJECTS_CSV, OUTPUT_AVG_CSV, return_video_folder_name
from numpy import dot
from numpy.linalg import norm
from ..utils_module.utils import returnIntIfPossible
import warnings
warnings.filterwarnings("error")

def cosine_similarity(v1,v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
    try:
        return dot(v1, v2)/(norm(v1,ord=2)*norm(v2,ord=2))
    except RuntimeWarning:
        return "NaN"

def generateOutputAvg(video_id):
    '''Generate output avg csv file for a video'''
    captions_and_objects_csv = return_video_folder_name(video_id)+'/'+CAPTIONS_AND_OBJECTS_CSV
    output_avg_csv = return_video_folder_name(video_id)+'/'+OUTPUT_AVG_CSV
    jsonArray = []
    with open(captions_and_objects_csv, 'r') as csvFile:
        csvReader = csv.DictReader(csvFile) 
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
        isKeyFrame = []
        description = []
        frame_index = []
        timestamp = []
        list = []
        for row in jsonArray:
            keys = []
            for key in row:
                keys.append(key)
            temp = []
            isKeyFrame.append(row[keys[2]])
            description.append(row[keys[3]])
            frame_index.append(returnIntIfPossible(float(row[keys[0]])))
            timestamp.append(returnIntIfPossible(float(row[keys[1]])))
            for idx in range(4,len(keys)):
                if(row[keys[idx]] != ''):
                    temp.append(float(row[keys[idx]]))
                else:
                    temp.append(0.0)
            list.append(temp)
        data = []
        for idx in range(2,len(list) - 1):
            s = returnIntIfPossible(cosine_similarity(list[idx],list[idx+1]))
            a1 = None
            a2 = None
            if(idx < len(list) - 3):
                a1 = returnIntIfPossible(cosine_similarity(list[idx-1],list[idx+2]))
                a2 = returnIntIfPossible(cosine_similarity(list[idx-2],list[idx+3]))
            else:
                a1 = 0.0
                a2 = 0.0
            if(s == "NaN"):
                s = "SKIP"
            data.append([frame_index[idx],timestamp[idx],idx,idx+1,s,a1,a2,isKeyFrame[idx],description[idx]])
        with open(output_avg_csv, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(['frame','timestamp','Line1','Line2','Similarity','avgone','avgtwo','iskeyFrame','description'])
            writer.writerows(data)
            print("Output avg csv file generated for video: ", video_id)
                
if __name__ == "__main__":
    generateOutputAvg('upSnt11tngE')