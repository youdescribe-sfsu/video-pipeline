
# YouDescribeX Pipeline

  

This is a description of the YouDescribeX Pipeline, which includes the steps necessary to generate descriptions for videos using a variety of computer vision and natural language processing tools.

<a href="#save-and-load-progress-mechanism"><font size="6">Jump to Save and Load Mechanism</font></a>
  

![Pipeline Graph](https://drive.google.com/uc?export=view&id=1WpUQ4XwMI56S2LMgypIC7QtzQFGKYZ60)

  

## PipelineRunner

  

The PipelineRunner is the entry point for the whole process. It is responsible for calling services, in the order needed, aggregating results and uploading them to the YouDescribeX site when done. The input for this service is the video id, and the output is the `description.json` file that is sent to YDX.

  

## Import Video Module

  

The ImportVideo service downloads the video from YouTube in .mp4 format and saves it in `/home/datasets/pipeline/' + video_id + '_files'`. The video is saved in .mp4 format, and the input for this service is the video id. The output is the .mp4 file.

  

## Extract Audio Module

  

The ExtractAudio service uses FFMPEG to extract the audio from the video in .flac format, which is a requirement from Google. The audio file is saved in the same directory as the video file. This service depends on Import Video, and the input is the .mp4 file. The output is the .flac file.

  

## Speech To Text Module

  

The Speech To Text service sends the .flac audio file to the Google Speech-to-Text service and gets back a JSON file with timestamps and audio tracks’ dialogue. This service depends on ExtractAudio, and the input is the .flac file. The output is the .json file.

  

## Frame Extraction Module

  

The Frame Extraction Module service extracts all of the frames of the video using cv2 and saves the frames into a “frames” folder under the main directory for the video. This service depends on DownloadVideo, and the input is the .mp4 file. The service extracts frames at a default rate of 10fps and saves them in .jpg format. The output is the .jpg files.

  

## OCR Module

  

The OCR service uses the Google service to recognize text in the frames that were extracted in the previous step. The service uploads the frames as .jpg to the OCR service and gets back a .json file as output. The service receives as many lines in output as images that were sent. This service depends on FrameExtraction, and the input is the frames in a loop. The output is the .json file.

  

## Object Detection Module

  

The ObjectDetection service uses the yolov3 service hosted on the school GPU server to identify objects in each frame of the video. This service depends on FrameExtraction, and it takes a single image file or URL as input and returns a list of identified objects. The service operates on one image at a time, and there is an option to use a batch size to improve performance. The output is text.

  

## Scene Segmentation Module

  

The SceneSegmentation service identifies the different scenes in the video based on the average cosine similarity of 3 windows of varying sizes. If the averages are below a threshold, then a new scene is considered. The descriptions from the OCR service are combined into this scene. The output is a JSON file with scene begin and end timestamp and list of descriptions corresponding to that scene.

  
  

## Image Captioning Module

  

The ImageCaptioning service currently uses Pythia but the team plans to upgrade to MMF and/or swap out with other drop-in replacements. It does not currently do batching and runs locally on a GPU server via a web.py service.

  
  

## Caption Rating Module

  

The CaptionRating service does not currently exist, but the team plans to create a web.py service that takes in an image-caption pair and returns a rating. The service will need to instantiate a vilbert pretrained model for extracting embeddings and a trained iicr model for rating the image-caption embedding pair. This service is used for filtering out bad captions from the summary.

  
  

## Caption Filtering Module

  

The CaptionFiltering service uses the output from CaptionRating and creates an aggregated, filtered set of keyframes in CSV format. The service takes a list of captions and ratings as input and outputs a CSV list of keyframe images, captions, and their ratings. The team plans to determine how the filtering will work, whether to use a threshold or drop a percentage, and will leverage the work of Ilmi.

  

## Text Summarization Module

  

The TextSummarization service aggregates and coalesces the list of descriptions by calculating average BLEU1-4 scores, grouping similar sentences together, then taking the best within some threshold.

  

## Upload to YouDescribeX

  

The UploadToYouDescribe module is the last step in the pipeline and depends on the TextSummarization module. This module aggregates all artifacts produced by the previous steps and uploads them to YouDescribe, a platform for creating and sharing video descriptions. The artifacts include the original MP4 video file, a FLAC audio file, and several CSV files containing information on images, objects, captions, scenes, and OCR outputs. In addition, it generates JSON files with text summaries of the video's scenes and transcripts of the audio. The module logs the success of the upload process to a file. It is important to thoroughly test the module with small files and edge cases to ensure that it functions properly before uploading larger files.

  
  
  

# Running In Development
Install dependencies :

    pip install -r requirements.txt
 
 Create a .env with following config:

    ANDREW_YOLO_UPLOAD_URL='http://localhost:8081'
    ANDREW_YOLO_TOKEN='ASK ANDREW FOR TOKEN'
    GPU_LOCAL_PORT='8080'
    YDX_WEB_SERVER = 'https://ydx.youdescribe.org'
    YDX_USER_ID = "UUID FOR YDX"
    YDX_AI_USER_ID = "UUID FOR YDX AI"
    CURRENT_ENV = "development"

Run the Pipeline with the following code:

    python pipeline_runner.py --video_id <INSERT_YT_VIDEO_ID>
   
   You can pass additional Configs for testing:
   

	   start_time : start time of youtube video to cut in seconds
       end_time: end time of youtube video to cut in seconds
       upload_to_server: Pass this if you want to upload to YDX server

## Save and Load Progress Mechanism
### Overview
In this project, we've implemented a save and load progress mechanism that allows us to track the progress of various tasks associated with video processing and analysis. This mechanism ensures that even if the program is interrupted or closed, we can resume from where we left off without losing any progress data. The progress data is stored in JSON format in separate files associated with each video.

### How it Works
We've defined a set of functions that handle the saving and loading of progress data, as well as updating specific values within the progress data.

#### `load_progress_from_file(video_runner_obj: Dict[str, int]) -> Dict or None`
This function loads progress data from a JSON file associated with a specific video. If the file doesn't exist, it starts with a default progress dictionary.

#### `read_value_from_file(video_runner_obj: Dict[str, int], key: str) -> Dict or None`
This function reads a specific value from the progress data stored in a JSON file based on the provided video runner object and key.

#### `save_progress_to_file(video_runner_obj: Dict[str, int], progress_data: Dict[str, int])`
This function saves progress data to a JSON file associated with a specific video runner object.

#### `save_value_to_file(video_runner_obj: Dict[str, int], key: str, value: Any) -> None`
This function saves a new value associated with a specific key to the progress data stored in a JSON file for the given video runner object.

### Default Progress Data
We've defined a default progress data dictionary named `DEFAULT_SAVE_PROGRESS`, which contains various keys and subkeys to track the progress of different tasks related to video analysis. This dictionary structure helps us organize and manage the progress information effectively.

### Usage
Here's how you can utilize the save and load progress mechanism in your code:

1. **Loading Progress Data**
   To load progress data associated with a specific video, use the `load_progress_from_file(video_runner_obj)` function. It returns the loaded progress dictionary or the default progress data if the file doesn't exist.

2. **Reading Specific Values**
   If you want to retrieve a specific value from the progress data, use the `read_value_from_file(video_runner_obj, key)` function. Provide the video runner object and the key corresponding to the value you're interested in.

3. **Saving Progress Data**
   To save progress data back to the file, use the `save_progress_to_file(video_runner_obj, progress_data)` function. Provide the video runner object and the updated progress data dictionary.

4. **Saving Specific Values**
   If you need to update a specific value in the progress data and save it back to the file, use the `save_value_to_file(video_runner_obj, key, value)` function. Provide the video runner object, the key, and the new value you want to associate with the key.

### Database Usage for Task Tracking

The YouDescribeX Pipeline utilizes a SQLite database to track the progress of tasks. This helps manage multiple video processing tasks and ensures that task progress is saved, even if the program is interrupted or closed.

#### Database Structure

The database comprises two tables:

1. **youtube_data:**
   - Stores YouTube ID, AI user ID, and task status.

2. **ai_user_data:**
   - Stores additional information for each task, including user ID, YouTube ID, AI user ID, YDX server, YDX app host, and status.

#### Functions:

1. **Create the Database**
   - `create_database()`: Creates the SQLite database and necessary tables if they don't exist.

2. **Adding New Tasks**
   - `process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id)`: Adds a new task to the database.
   - If the YouTube ID and AI user ID combination already exists, a new row is added to the `ai_user_data` table.

3. **Updating Task Status**
   - `update_status(youtube_id, ai_user_id, status)`: Updates the task status in the `youtube_data` table.
   - `update_ai_user_data(youtube_id, ai_user_id, user_id, status)`: Updates the task status in the `ai_user_data` table.

4. **Retrieving Task Information**
   - `get_pending_jobs_with_youtube_ids()`: Retrieves YouTube ID and AI user ID of tasks in progress.
   - `get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id)`: Retrieves all data for a specific YouTube ID and AI user ID.
   - `get_data_for_youtube_id_ai_user_id(youtube_id, ai_user_id)`: Retrieves the YDX server and YDX app host for a specific YouTube ID and AI user ID.