# Extract the frames from a downloaded video file

import cv2
import os
from utils import returnVideoDownloadLocation,returnVideoFramesFolder

def extract_frames(video_file, frames_per_second, logging=False,start_time=None,end_time=None):
	"""
	Extracts frames from a given video_file and puts them in a folder with the
	same name as the video file minus the extension.
	frames_per_second is the target number of frames per second to extract.
	This value is rounded to be an exact divisor of the video frame rate.
	"""
	vid_name = returnVideoFramesFolder(video_file,start_time=start_time,end_time=end_time)
	
	if not os.path.exists(vid_name):
		try:
			os.mkdir(vid_name)
		except OSError:
			print('Cannot create directory for frames')
			return
	# Open the video handler and get fps
	vid = cv2.VideoCapture(returnVideoDownloadLocation(video_file,start_time=start_time,end_time=end_time))
	
	fps = round(vid.get(cv2.CAP_PROP_FPS))
	frames_per_extraction = round(fps / frames_per_second)
	num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
	list = os.listdir(vid_name)

	
	if len(list) < (num_frames/frames_per_extraction):
		if logging:
			print("Extracting frames from {} ({} fps, {} frames)...".format(video_file, fps, num_frames))
		
		frame_count = 0
		while frame_count < num_frames:
			status, frame = vid.read()
			if not status:
				print("Error extracting frame {}.".format(frame_count))
				break
			if frame_count % frames_per_extraction == 0:
				# save the frame
				cv2.imwrite("{}/frame_{}.jpg".format(vid_name, frame_count), frame)
			if logging:
				print('\r{}% complete  '.format((frame_count*100)//num_frames), end='')
			frame_count += 1
		if logging:
			print('\r100% complete   ')
		vid.release()
		
		# Create a data file to accompany the frames to keep track of the total
		# number of frames and the number of video frames per extracted frame
		actual_frames_per_second = fps / frames_per_extraction
		with open('{}/data.txt'.format(vid_name), 'w') as datafile:
			datafile.write('{} {} {}\n'.format(frames_per_extraction, frame_count, actual_frames_per_second))
	else:
		print("Frames extracted, skipping step")
if __name__ == "__main__":
	# video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
	# video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
	# video_name = 'Good Samaritans knew that this puppy needed extra help'
	# video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
	# video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
	# video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
	video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'
	
	extract_frames(video_name + '.mp4', 10, True)
