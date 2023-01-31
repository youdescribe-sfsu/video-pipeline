import csv
from utils import OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, returnVideoFolderName


columns = {
  "start_time": "start_time",
  "end_time": "end_time",
  "description": "description",
}

def averageCheck(averageone, averagetwo, threshold):
    if (averageone < threshold and averagetwo < threshold):
        return True
    else: 
        return False

def segmentedData(sceneTimeLimit, threshold,list_new):
    scenesegments = []
    currentSceneTimeStamp = 0
    firstSkip = False
    skiptimestamp = None
    description = ""
    data = []
    
    for i in range(len(list_new)):
        if(list_new[i][7] == 'True'):
            description = description +  "\n" + list_new[i][8]
        if(list_new[i][4] != 'SKIP' and list_new[i][4] < threshold):
            if(averageCheck(list_new[i][5], list_new[i][6], threshold) and
            list_new[i][1] - currentSceneTimeStamp > sceneTimeLimit):
                scenesegments.append(list_new[i][1])
                data.append([currentSceneTimeStamp, list_new[i][1], description])
                description = ""
                currentSceneTimeStamp = list_new[i][1]
        if(list_new[i][4] != 'SKIP' and firstSkip == True):
            if (list_new[i][1] - skiptimestamp >= sceneTimeLimit):
                scenesegments.append(list_new[i][1])
                data.append([currentSceneTimeStamp, list_new[i][1], description])
                description = " "
                currentSceneTimeStamp = list_new[i][1]
            firstSkip = False
        if(list_new[i][4] == 'SKIP'):
            if(firstSkip == False):
                skiptimestamp = list_new[i][1]
                firstSkip = True
    
    return data

def parseCSVFile(csvPath):
    headers=[]
    list_new = []
    with open(csvPath, 'r') as csvFile:
        reader = csv.reader(csvFile)
        headers = next(reader)
        print(len(headers))
        for row in reader:
            print(row)
            temp = []
            for idx in range(len(headers)):
                if(row[idx] == ""):
                    temp.append(0.0)
                else:
                    if(idx == 4):
                        if(row[idx] == "SKIP"):
                            temp.append(row[idx])
                        else:
                            temp.append(float(row[idx]))
                    elif(idx == 7 or idx == 8):
                        temp.append(row[idx])
                    else:
                        temp.append(float(row[idx]))
            list_new.append(temp)
    return list_new

def sceneSegmentation(video_id):
    '''Segment the video into scenes based on the average of the scene and the average of the shot.'''
    outputavgFile = returnVideoFolderName(video_id) +'/'+ OUTPUT_AVG_CSV
    sceneSegmentedFile = returnVideoFolderName(video_id) +'/'+ SCENE_SEGMENTED_FILE_CSV
    list_new = parseCSVFile(outputavgFile)
    data = segmentedData(10, 0.75,list_new)
    with open(sceneSegmentedFile, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(columns.values())
        writer.writerows(data)
    return