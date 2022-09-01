# Use Google Cloud Vision API to extract on screen text through OCR

import io
import os
import csv
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="tts_cloud_key.json"
# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision_v1 import types

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
		texts = response.text_annotations
		return texts
	except:
		client = vision.ImageAnnotatorClient()
		

		with io.open(path, 'rb') as image_file:
			content = image_file.read()

		image = types.Image(content=content)

		response = client.text_detection(image=image)
		texts = response.text_annotations
		return texts


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

def print_all_ocr(video_name, start=0):
	"""
	Writes out all detected text for each extracted frame into a csv file
	TODO(Lothar): Keep track of bounding boxes
	"""
	video_name = video_name.split('/')[-1].split('.')[0]
	with open('{}/data.txt'.format(video_name), 'r') as datafile:
		data = datafile.readline().split()
		step = int(data[0])
		num_frames = int(data[1])
		frames_per_second = float(data[2])
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
	outcsvpath = "OCR Text" + ".csv"
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
				writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
			for frame_index in range(start, num_frames, step):				
				frame_filename = '{}/frame_{}.jpg'.format(video_name, frame_index)
				texts = detect_text(frame_filename)
				if len(texts) > 0:
					new_row = [frame_index, float(frame_index)*seconds_per_frame, texts[0].description]
					print(frame_index)
					print(float(frame_index)*seconds_per_frame)
					#print(texts[0].description)
					writer.writerow(new_row)
					outcsvfile.flush()

if __name__ == "__main__":
	# video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
	# video_name = 'Good Samaritans knew that this puppy needed extra help'
	# video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
	# video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
	# video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
	# video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
	video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'

	print_all_ocr(video_name)
