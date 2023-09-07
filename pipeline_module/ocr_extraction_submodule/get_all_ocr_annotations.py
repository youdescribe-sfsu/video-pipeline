# Use Google Cloud Vision API to extract on screen text through OCR

import io
import os
import csv
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="tts_cloud_key.json"
# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision_v1 import types
from ..utils_module.utils import OCR_TEXT_ANNOTATIONS_FILE_NAME, load_progress_from_file, read_value_from_file, return_video_frames_folder,return_video_folder_name,OCR_HEADERS,FRAME_INDEX_SELECTOR,TIMESTAMP_SELECTOR,OCR_TEXT_SELECTOR, save_progress_to_file, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from google.cloud.vision_v1 import AnnotateImageResponse
import json
from typing import Dict

# def get_ocr_progress(video_runner_obj):
#     """Get the OCR progress data for a specific video."""
#     save_data = load_progress_from_file(video_runner_obj=video_runner_obj)
#     return save_data

# def update_ocr_progress(video_runner_obj,save_data ,frame_index):
#     """Update the OCR progress data for a specific video."""
#     progress_data = save_data
#     progress_data["FrameExtraction"]["extract_frames"] = frame_index
#     save_progress_to_file(video_runner_obj=video_runner_obj, progress_data=progress_data)
#     return


def detect_text(path: str) -> Dict:
    """
    Detects text in an image file and returns a dictionary of the response.
    
    Parameters:
    path (str): The file path of the image.
    
    Returns:
    Dict: The dictionary of the response from the Google Cloud Vision API.
    
    Raises:
    Exception: If the text detection fails, the error message is printed.
    """
    try:
        client = vision.ImageAnnotatorClient()
        with open(path, 'rb') as image_file:
            content = image_file.read()

        image = types.Image(content=content)

        response = client.text_detection(image=image)
        response_json = AnnotateImageResponse.to_json(response)
        response = json.loads(response_json)
        return response
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {}


def detect_text_uri(uri):
	"""
	Detects text in the file located in Google Cloud Storage or on the Web.
	"""
	client = vision.ImageAnnotatorClient()
	image = types.Image()
	image.source.image_uri = uri

	response = client.text_detection(image=image)
	texts = response.text_annotations
	return texts
	
def get_ocr_confidences(video_runner_obj):
	"""
	Attempts to grab confidence data from the API
	NOTE: Does not actually work - always returns 0.0
	"""
	video_frames_folder = return_video_frames_folder(video_runner_obj)
	# with open('{}/data.txt'.format(video_frames_folder), 'r') as datafile:
	# 	data = datafile.readline().split()
	# 	step = int(data[0])
	# 	num_frames = int(data[1])
	# 	frames_per_second = float(data[2])
	# save_file = load_progress_from_file(video_runner_obj=video_runner_obj)
	# step = save_file['video_common_values']['frames_per_extraction']
	step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['frames_per_extraction']")
	# num_frames = save_file['video_common_values']['num_frames']
	num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
	# frames_per_second = save_file['video_common_values']['actual_frames_per_second']
	frames_per_second = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['actual_frames_per_second']")
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
	outcsvpath = "OCR Confidences - " + video_runner_obj['video_id'] + ".csv"
	with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
		writer = csv.writer(outcsvfile)
		writer.writerow(["Frame Index", "Confidence", "OCR Text"])
		for frame_index in range(0, num_frames, step):
			frame_filename = '{}/frame_{}.jpg'.format(video_frames_folder, frame_index)
			texts = detect_text(frame_filename)
			if len(texts) > 0:
				new_row = [frame_index, texts[0].confidence, texts[0].description]
				video_runner_obj.logger.info(f"Frame Index: {frame_index}")
				video_runner_obj.logger.info(f"Timestamp: {float(frame_index)*seconds_per_frame}")
				video_runner_obj.logger.info(f"Confidence: {texts[0].confidence}")
				video_runner_obj.logger.info(f"OCR Text: {texts[0].description}")
				writer.writerow(new_row)


## TODO: Implement Batch OCR
@timeit
def get_all_ocr_annotations(video_runner_obj, start=0):
	"""
    Writes out all detected text from OCR for each extracted frame in a video to a csv file. 
    The function resumes the progress if the csv file already exists and contains data.

    Args:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
    start (int, optional): The starting frame index to extract OCR annotations from. Defaults to 0.

    Returns:
    None

    TODO:
    Keep track of bounding boxes for each OCR annotation.
    """
	video_frames_folder = return_video_frames_folder(video_runner_obj)
	video_runner_obj["logger"].info(f"Getting all OCR annotations for {video_runner_obj['video_id']}")
	video_runner_obj["logger"].info(f"video_frames_folder={video_frames_folder}")

	if read_value_from_file(video_runner_obj=video_runner_obj, key="['OCR']['started']") == False:
		save_value_to_file(video_runner_obj=video_runner_obj, key="['OCR']['started']", value=True)
  
		step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")
		num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
		frames_per_second = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['frames_per_second']")
		start = 0
  
		save_value_to_file(video_runner_obj=video_runner_obj, key="['OCR']['start']", value=start)

	else:
		step = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['step']")
		num_frames = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['num_frames']")
		frames_per_second = read_value_from_file(video_runner_obj=video_runner_obj, key="['video_common_values']['frames_per_second']")
		start = read_value_from_file(video_runner_obj=video_runner_obj, key="['OCR']['start']")
		
	
	# Calculate video fps and seconds per frame
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
 
 	# Path to the csv file where OCR annotations will be written
	outcsvpath = return_video_folder_name(video_runner_obj=video_runner_obj)+ "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME

	if start != 0:
		mode = 'a'
	else:
		mode = 'w'

	if start < num_frames-step:
		with open(outcsvpath, mode, newline='', encoding='utf-8') as outcsvfile:
			
			writer = csv.writer(outcsvfile)
			if(start == 0):
				writer.writerow([OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])
			for frame_index in range(start, num_frames, step):				
				frame_filename = '{}/frame_{}.jpg'.format(video_frames_folder, frame_index)
				video_runner_obj["logger"].info(f"Frame Index: {frame_index}")
				texts = detect_text(frame_filename)
				if len(texts) > 0:
					try:
						new_row = [frame_index, float(frame_index)*seconds_per_frame, json.dumps(texts)]
						video_runner_obj["logger"].info(f"Timestamp: {float(frame_index)*seconds_per_frame}")
						video_runner_obj["logger"].info(f"Frame Index : {frame_index}")
						writer.writerow(new_row)
						outcsvfile.flush()
					except Exception as e:
						print(e)
						video_runner_obj["logger"].info(f"Error writing to file")
				# progress_file['OCR']['start'] = frame_index
				# save_progress_to_file(video_runner_obj=video_runner_obj, progress_data=progress_file)
				save_value_to_file(video_runner_obj=video_runner_obj, key="['OCR']['start']", value=frame_index)
        
if __name__ == "__main__":
	# video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
	# video_name = 'Good Samaritans knew that this puppy needed extra help'
	# video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
	# video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
	# video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
	# video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
	# video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'

	# print_all_ocr(video_name)
 	# response = detect_text("upSnt11tngE_files/frames/frame_1788.jpg")
	get_all_ocr_annotations("upSnt11tngE")

	#python full_video_pipeline.py --videoid dgrKawK-Kjc 2>&1 | tee dgrKawK-Kjc.log