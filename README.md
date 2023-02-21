# YouDescribeX Pipeline

This is a description of the YouDescribeX Pipeline, which includes the steps necessary to generate descriptions for videos using a variety of computer vision and natural language processing tools.

![Pipeline Graph](https://drive.google.com/file/d/1WpUQ4XwMI56S2LMgypIC7QtzQFGKYZ60/view?usp=sharing)

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

## SceneSegmentation

The SceneSegmentation service identifies the different scenes in the video based on the average cosine similarity of 3 windows of varying sizes. If the averages are below a threshold, then a new scene is considered. The descriptions from the OCR service are combined into this scene. The output is a JSON file with scene begin and end timestamp and list of descriptions corresponding to that scene.

## KeyframeSelection

The KeyframeSelection service selects keyframes from the list of frames, based on the number of confident detections. The service uses the entire list of frames after ObjectDetection as input via CSV. The output is a CSV with frame number and timestamps.


## ImageCaptioning

The ImageCaptioning service currently uses Pythia but the team plans to upgrade to MMF and/or swap out with other drop-in replacements. It does not currently do batching and runs locally on a GPU server via a web.py service.


## CaptionRating

The CaptionRating service does not currently exist, but the team plans to create a web.py service that takes in an image-caption pair and returns a rating. The service will need to instantiate a vilbert pretrained model for extracting embeddings and a trained iicr model for rating the image-caption embedding pair. This service is used for filtering out bad captions from the summary.


## CaptionFiltering

The CaptionFiltering service uses the output from CaptionRating and creates an aggregated, filtered set of keyframes in CSV format. The service takes a list of captions and ratings as input and outputs a CSV list of keyframe images, captions, and their ratings. The team plans to determine how the filtering will work, whether to use a threshold or drop a percentage, and will leverage the work of Ilmi.

## TextSummarization

The TextSummarization service aggregates and coalesces the list of descriptions by calculating average BLEU1-4 scores, grouping similar sentences together, then taking the best within some threshold.

## Upload to YouDescribeX

The UploadToYouDescribe module is the last step in the pipeline and depends on the TextSummarization module. This module aggregates all artifacts produced by the previous steps and uploads them to YouDescribe, a platform for creating and sharing video descriptions. The artifacts include the original MP4 video file, a FLAC audio file, and several CSV files containing information on images, objects, captions, scenes, and OCR outputs. In addition, it generates JSON files with text summaries of the video's scenes and transcripts of the audio. The module logs the success of the upload process to a file. It is important to thoroughly test the module with small files and edge cases to ensure that it functions properly before uploading larger files.


