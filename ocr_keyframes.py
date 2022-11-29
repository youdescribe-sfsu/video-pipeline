# Use Google Cloud Vision API to extract on screen text through OCR

import io
import os
import csv
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="tts_cloud_key.json"
# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision_v1 import types
from utils import OCR_TEXT_ANNOTATIONS_FILE_NAME, returnVideoFramesFolder,returnVideoFolderName,OCR_TEXT_CSV_FILE_NAME,COUNT_VERTICE
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
		with open('sample.json', 'w', encoding='utf-8') as jsonf: 
			jsonString = json.dumps(response,indent=4)
			jsonf.write(jsonString)
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

@timeit
def print_all_ocr_annotations(video_id, start=0):
	"""
	Writes out all detected text for each extracted frame into a csv file
	TODO(Lothar): Keep track of bounding boxes
	"""
	video_name = returnVideoFramesFolder(video_id)
	print("--------------------------")
	print(video_name)
	print("--------------------------")

	# video_name = video_name.split('/')[-1].split('.')[0]
	with open('{}/data.txt'.format(video_name), 'r') as datafile:
		data = datafile.readline().split()
		step = int(data[0])
		num_frames = int(data[1])
		frames_per_second = float(data[2])
	video_fps = step * frames_per_second
	seconds_per_frame = 1.0/video_fps
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
				writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
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

def replace_all(text_description, description_to_remove):
    if(description_to_remove is not None):
        for description in description_to_remove:
            text_description = text_description.replace(description, "")
    return text_description

@timeit
def print_all_ocr(video_id):
    annotation_file = open(returnVideoFolderName(video_id)+"/"+COUNT_VERTICE)
    annotation_file_json = json.load(annotation_file)
    max_count_annotation = annotation_file_json[0]
    annotation_file.close()
    outcsvpath = returnVideoFolderName(video_id)+ "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
    description_to_remove = None
    if(max_count_annotation["percentage"] > 60):
        description_to_remove = max_count_annotation["description"]
    ocr_text_csv = returnVideoFolderName(video_id)+ "/" + OCR_TEXT_CSV_FILE_NAME
    ocr_text_csv_file = open(ocr_text_csv, 'w', newline='', encoding='utf-8')
    ocr_text_csv_writer = csv.writer(ocr_text_csv_file)
    ocr_text_csv_writer.writerow(["Frame Index", "Timestamp", "OCR Text"])
    with open(outcsvpath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf) 
        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            ocr_text = json.loads(row["OCR Text"])
            frame_index = row["Frame Index"]
            timestamp = row["Timestamp"]
            if(len(ocr_text['textAnnotations']) > 0):
                text_description = ocr_text['textAnnotations'][0]['description']
                replaced_text_description = replace_all(text_description, description_to_remove)
                if(len(replaced_text_description) > 0):
                    ocr_text_csv_writer.writerow([frame_index, timestamp, replaced_text_description])
                    ocr_text_csv_file.flush()
        ocr_text_csv_file.close()
        
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
	print_all_ocr("upSnt11tngE")

#python full_video_pipeline.py --videoid dgrKawK-Kjc 2>&1 | tee dgrKawK-Kjc.log