# Optimize keyframe selection based on object detection results

import csv

def keyframes_from_object_tracking(video_name, target_keyframes_per_second=1):
	"""
	Iteratively selects the keyframe that has the highest sum of square
	confidences and is reasonably close to 1/target_keyframes_per_second seconds
	after the previous keyframe
	"""
	video_name = video_name.split('/')[-1].split('.')[0]
	with open('{}/data.txt'.format(video_name), 'r') as datafile:
		data = datafile.readline().split()
		step = int(data[0])
		num_frames = int(data[1])
		frames_per_second = float(data[2])
	
	incsvpath = 'Objects.csv'
	with open(incsvpath, newline='', encoding='utf-8') as incsvfile:
		reader = csv.reader(incsvfile)
		header = next(reader) # skip header
		rows = [row for row in reader]
	
	frame_values = []
	for row in rows:
		frame_index = int(row[0])
		weights = [float(x) for x in row[1::2] if x != '']
		value = sum([x*x for x in weights])
		frame_values.append((frame_index, value))
	
	video_fps = step * frames_per_second
	frames_per_target_period = video_fps / target_keyframes_per_second
	keyframes = []
	last_keyframe = -step
	for (index, value) in frame_values:
		if index - last_keyframe > 2*frames_per_target_period or index + step >= num_frames:
			window = frame_values[last_keyframe//step + 1:index//step]
			width = index - last_keyframe
			a = -4.0/(width*width)
			b = 4.0/width
			best = -1
			best_val = -1.0
			for (index_w, value_w) in window:
				rel_index = index_w - last_keyframe
				coeff = a*rel_index*rel_index + b*rel_index
				modified_value = coeff*value_w
				if modified_value >= best_val:
					best = index_w
					best_val = modified_value
			keyframes.append(best)
			last_keyframe = best
	
	seconds_per_frame = 1.0/video_fps
	outcsvpath = 'Keyframes.csv'
	with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
		writer = csv.writer(outcsvfile)
		writer.writerow(["Frame Index", "Timestamp"])
		for frame_index in keyframes:
			new_row = [frame_index, float(frame_index)*seconds_per_frame]
			print(frame_index, float(frame_index)*seconds_per_frame)
			writer.writerow(new_row)

if __name__ == "__main__":
	target_kfps = 0.167
	# video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
	# keyframes_from_object_tracking(video_name, target_kfps)
	# video_name = 'Good Samaritans knew that this puppy needed extra help'
	# keyframes_from_object_tracking(video_name, target_kfps)
	# video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
	# keyframes_from_object_tracking(video_name, target_kfps)
	# video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
	# keyframes_from_object_tracking(video_name, target_kfps)
	# video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
	# keyframes_from_object_tracking(video_name, target_kfps)
	# video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
	# keyframes_from_object_tracking(video_name, target_kfps)
	video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'
	keyframes_from_object_tracking(video_name, target_kfps)
