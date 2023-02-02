# Use Google Cloud Vision API to extract on screen text through OCR

import io
import os
import csv
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="tts_cloud_key.json"
# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision_v1 import types
from utils import OCR_TEXT_ANNOTATIONS_FILE_NAME, returnVideoFramesFolder,returnVideoFolderName,OCR_HEADERS,FRAME_INDEX_SELECTOR,TIMESTAMP_SELECTOR,OCR_TEXT_SELECTOR
from timeit_decorator import timeit
from google.cloud.vision_v1 import AnnotateImageResponse
import json

def detect_text(path):
	"""
	Detects text in the file
	"""
	try:
		client = vision.ImageAnnotatorClient()
		with io.open(path, 'rb') as image_file:
			content = image_file.read()

		image = types.Image(content=content)

		response = client.text_detection(image=image)
		response_json = AnnotateImageResponse.to_json(response)
		response = json.loads(response_json)
		return response
	except Exception as e:
		print("ERROR======================",str(e))
		client = vision.ImageAnnotatorClient()
		with io.open(path, 'rb') as image_file:
			content = image_file.read()

		image = types.Image(content=content)

		response = client.text_detection(image=image)
		response_json = AnnotateImageResponse.to_json(response)
		response = json.loads(response_json)
		return response


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
	
def get_ocr_confidences(video_name):
	"""
	Attempts to grab confidence data from the API
	NOTE: Does not actually work - always returns 0.0
	"""
	with open('{}/data.txt'.format(video_name), 'r') as datafile:
		data = datafile.readline().split()
		step = int(data[0])
		num_frames = int(data[1])
		frames_per_second = float(data[2])
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
	outcsvpath = "OCR Confidences - " + video_name + ".csv"
	with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
		writer = csv.writer(outcsvfile)
		writer.writerow(["Frame Index", "Confidence", "OCR Text"])
		for frame_index in range(0, num_frames, step):
			frame_filename = '{}/frame_{}.jpg'.format(video_name, frame_index)
			texts = detect_text(frame_filename)
			if len(texts) > 0:
				new_row = [frame_index, texts[0].confidence, texts[0].description]
				print(frame_index)
				print(float(frame_index)*seconds_per_frame)
				print(texts[0].description)
				print()
				writer.writerow(new_row)


## TODO: Implement Batch OCR
@timeit
def get_all_ocr_annotations(video_id, start=0):
	"""
    Writes out all detected text from OCR for each extracted frame in a video to a csv file. 
    The function resumes the progress if the csv file already exists and contains data.

    Args:
    video_id (str): The id of the video to extract OCR annotations from.
    start (int, optional): The starting frame index to extract OCR annotations from. Defaults to 0.

    Returns:
    None

    TODO:
    Keep track of bounding boxes for each OCR annotation.
    """
	video_name = returnVideoFramesFolder(video_id)
	print("--------------------------")
	print(video_name)
	print("--------------------------")

	# video_name = video_name.split('/')[-1].split('.')[0]
 	# Read data for the video
	with open('{}/data.txt'.format(video_name), 'r') as datafile:
		data = datafile.readline().split()
		step = int(data[0])
		num_frames = int(data[1])
		frames_per_second = float(data[2])
  
	# Calculate video fps and seconds per frame
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
 
 	# Path to the csv file where OCR annotations will be written
	outcsvpath = returnVideoFolderName(video_id)+ "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
 
	#check if file already contains progress from last attempt
	if os.path.exists(outcsvpath) :
		if os.stat(outcsvpath).st_size > 32:
			with open(outcsvpath, 'r', newline='', encoding='utf-8') as file:
				lines = file.readlines()
				lines.reverse()
				i = 0
				last_line = lines[i].split(",")[0]
				while not last_line.isnumeric():
					i+= 1
					last_line = lines[i].split(",")[0]			
				start = int(last_line)+step
				file.close()

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
				frame_filename = '{}/frame_{}.jpg'.format(video_name, frame_index)
				texts = detect_text(frame_filename)
				if len(texts) > 0:
					try:
						new_row = [frame_index, float(frame_index)*seconds_per_frame, json.dumps(texts)]
						print("Frame Index: ", frame_index)
						writer.writerow(new_row)
						outcsvfile.flush()
					except Exception as e:
						print(e)
						print("Error writing to file")
        
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