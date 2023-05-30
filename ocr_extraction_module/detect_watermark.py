from utils import return_video_folder_name,OCR_TEXT_ANNOTATIONS_FILE_NAME,COUNT_VERTICE
import csv 
import json

import sys

csv.field_size_limit(2**31-1)

def isSamePolygon(polygon1, polygon2):
    if(len(polygon1) != len(polygon2)):
        return False
    for i in range(len(polygon1)):
        if(abs(polygon1[i]["x"] - polygon2[i]["x"]) > 50 or abs(polygon1[i]["y"] - polygon2[i]["y"]) > 50):
            return False
    return True
        

def detect_watermark(video_runner_obj):
    """
    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
    """
    path = return_video_folder_name(video_runner_obj)+ "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
    # Maintain count of text in bouding box
    count_obj = []
      
    #read csv file
    with open(path, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 
        row_count = 0
        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            ocr_text = json.loads(row["OCR Text"])
            if(len(ocr_text["textAnnotations"]) > 0):
                row_count += 1
                text_annotations = ocr_text["textAnnotations"]
                for i in range(0,len(text_annotations)):
                    vertice = text_annotations[i]["boundingPoly"]["vertices"]
                    description = text_annotations[i]["description"]
                    locale = text_annotations[i]["locale"]
                    if(locale == "en" or len(locale) == 0):
                        found = False
                        for j in range(len(count_obj)):
                            if(isSamePolygon(vertice, count_obj[j]["vertice"])):
                                count_obj[j]["count"] += 1
                                if(description not in count_obj[j]["description"]):
                                    count_obj[j]["description"].append(description)
                                found = True
                                break
                        if(not found):
                            count_obj.append({
                                "vertice": vertice,
                                "description": [description],
                                "count": 1
                            })
        # print(count_obj)
        print("Total rows: ", row_count)   
        # Get Max count from count_obj
        count_obj = sorted(count_obj, key = lambda i: i['count'],reverse=True)
        if(len(count_obj) > 0):
            max_count = count_obj[0]["count"]
            vertice_with_max_count = count_obj[0]["vertice"]
            count_obj[0]['percentage'] = max_count/row_count*100
            print("Percentage of frames with watermark: ", max_count/row_count*100)
            print("Max count: ", max_count)
            print("Vertice with max count: ", vertice_with_max_count)
        
    with open(return_video_folder_name(video_runner_obj)+ "/" + COUNT_VERTICE, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(count_obj)
        jsonf.write(jsonString)
    return


if __name__ == "__main__":
    detect_watermark("upSnt11tngE")