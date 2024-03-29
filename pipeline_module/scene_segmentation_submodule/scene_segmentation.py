import csv
import json
from ..utils_module.utils import OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, load_progress_from_file, read_value_from_file, return_video_folder_name, save_progress_to_file, save_value_to_file
from .generate_average_output import generate_average_output

class SceneSegmentation:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj

    columns = {
        "start_time": "start_time",
        "end_time": "end_time",
        "description": "description",
    }

    def average_check(self,averageone, averagetwo, threshold):
        if averageone < threshold and averagetwo < threshold:
            return True
        else:
            return False

    def parse_CSV_file(self,csvPath):
        headers = []
        list_new = []
        with open(csvPath, "r") as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            for row in reader:
                temp = []
                for idx in range(len(headers)):
                    if row[idx] == "":
                        temp.append(0.0)
                    else:
                        if idx == 4:
                            if row[idx] == "SKIP":
                                temp.append(row[idx])
                            else:
                                temp.append(float(row[idx]))
                        elif idx == 7 or idx == 8:
                            temp.append(row[idx])
                        else:
                            temp.append(float(row[idx]))
                list_new.append(temp)
        return list_new

    def get_segmented_data(self, scene_time_limit, threshold, list_new):
        scenesegments = []
        currentSceneTimeStamp = 0
        firstSkip = False
        skiptimestamp = None
        description = ""
        data = []

        # frame,timestamp,Line1,Line2,Similarity,avgone,avgtwo,iskeyFrame,description
        # 6,0.2,2,3,SKIP,NaN,NaN,False,a dark dark dark dark dark dark sky
        # 9,0.3,3,4,0.8849123801434262,NaN,NaN,False,a close up picture of a purple mouse
        # 12,0.4,4,5,0.6998180926512969,0.28589822879945426,NaN,False,a close up of a blue and blue plate
        # 15,0.5,5,6,0.8511072730026329,0.7888871632771934,0.6271427476046002,True,a close up view of a blue and blue object
        # 18,0.6,6,7,0.8223277151843041,0.5891958064611101,0.6250482199849766,False,a close up of a blue and blue object

        for i in range(len(list_new)):
            ## If it is keyframe, add description to description
            if list_new[i][7] == "True":
                description = description + "\n" + list_new[i][8]
            ## If similarity exists, and the average similarities is less than threshold, and the time difference is greater than sceneTimeLimit
            if list_new[i][4] != "SKIP" and list_new[i][4] < threshold:
                if (
                    self.average_check(list_new[i][5], list_new[i][6], threshold)
                    and list_new[i][1] - currentSceneTimeStamp > scene_time_limit
                ):
                    scenesegments.append(list_new[i][1])
                    data.append([currentSceneTimeStamp, list_new[i][1], description])
                    description = ""
                    currentSceneTimeStamp = list_new[i][1]

            if list_new[i][4] != "SKIP" and firstSkip == True:
                if list_new[i][1] - skiptimestamp >= scene_time_limit:
                    scenesegments.append(list_new[i][1])
                    data.append([currentSceneTimeStamp, list_new[i][1], description])
                    description = " "
                    currentSceneTimeStamp = list_new[i][1]
                firstSkip = False
            if list_new[i][4] == "SKIP":
                if firstSkip == False:
                    skiptimestamp = list_new[i][1]
                    firstSkip = True

        return data
    
    

    def run_scene_segmentation(self):
        """Segment the video into scenes based on the average of the scene and the average of the shot."""
        self.video_runner_obj["logger"].info("Running scene segmentation")
        # save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        
        # if save_file['SceneSegmentation']['run_scene_segmentation'] == 1:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['SceneSegmentation']['run_scene_segmentation']") == 1:
            ## Already processed
            self.video_runner_obj["logger"].info("Already processed")
            print("Already processed")
            return
        
        
        
        # save_file.setdefault("SceneSegmentation", {})["started"] = True
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['SceneSegmentation']['started']", value=True)
        # if(save_file['SceneSegmentation']['generate_average_output'] == 1):
        if read_value_from_file(video_runner_obj=self.video_runner_obj,  key="['SceneSegmentation']['generate_average_output']") == 1:
            self.video_runner_obj["logger"].info("Already processed")
        else:    
            generate_average_output(self.video_runner_obj)
            # save_file['SceneSegmentation']['generate_average_output'] = 1
            # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['SceneSegmentation']['generate_average_output']", value=1)
        
        outputavgFile = return_video_folder_name(self.video_runner_obj) + "/" + OUTPUT_AVG_CSV
        sceneSegmentedFile = (
            return_video_folder_name(self.video_runner_obj) + "/" + SCENE_SEGMENTED_FILE_CSV
        )
        list_new = self.parse_CSV_file(outputavgFile)
        
        metadata = {}

        with open(return_video_folder_name(self.video_runner_obj) + "/metadata.json", "r") as f:
            metadata = json.load(f)
            
        video_time = metadata['duration']
        ## Threshold is an inverse of similarity
        ## Increase threshold to increase the number of scenes
        ## Total time of the video, and there needs to be a scene every 60-90 seconds
        
        optimal_threshold = self.incremental_search_for_optimal_threshold(0.75, 1.0, video_time, list_new)
        
        print("Optimal threshold: ", optimal_threshold)
        # print("Optimal scene time limit: ", optimal_scene_time_limit)
        
        data = self.get_segmented_data(10, optimal_threshold, list_new)
        with open(sceneSegmentedFile, "w") as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(self.columns.values())
            writer.writerows(data)
        self.video_runner_obj["logger"].info(f"Writing scene segmentation results to {sceneSegmentedFile}")
        # save_file['SceneSegmentation']['run_scene_segmentation'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['SceneSegmentation']['run_scene_segmentation']", value=1)
        print("Scene segmentation done")
        return



    def incremental_search_for_optimal_threshold(self, low, high, video_duration, list_new):
        increment = 0.05
        ## 25 seconds per scene
        optimal_number_of_scenes = video_duration // 25
                
        # Use a for loop for better readability
        for threshold in range(int(low * 100), int(high * 100) + 1, int(increment * 100)):
            threshold /= 100  # Convert back to float for calculations
            data = self.get_segmented_data(10, threshold, list_new)
            
            if len(data) >= optimal_number_of_scenes:
                return threshold
        
        # Return high if no threshold found
        return high