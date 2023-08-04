# Process the raw OCR into more useful output
import csv
from requests.api import request
from utils import OCR_HEADERS, return_video_folder_name,OCR_TEXT_CSV_FILE_NAME,OCR_FILTER_CSV_FILE_NAME,OCR_FILTER_CSV_2_FILE_NAME,OCR_FILTER_REMOVE_SIMILAR,OCR_TEXT_SELECTOR,TIMESTAMP_SELECTOR,FRAME_INDEX_SELECTOR
from timeit_decorator import timeit


def levenshtein_dist(source, target, subcost=1.0, delcost=1.0):
	"""
	Calculates the minimum number of deletions, additions, and substitutions
	to turn the source string into the target string
	"""
	m = len(source)
	n = len(target)
	if m > n: # Swap so as to use less memory
		source, target = target, source
		m, n = n, m
	d = [i * delcost for i in range(m + 1)]
	for j in range(1, n + 1):
		x = d[0]
		d[0] = j * delcost
		for i in range(1, m + 1):
			new_di = min(d[i - 1] + delcost,
			             d[i] + delcost,
			             x + (0.0 if source[i - 1] == target[j - 1] else subcost))
			x = d[i]
			d[i] = new_di
	return d[m]


@timeit
def filter_ocr(video_runner_obj, window_width=10, threshold=0.5):
	"""
	Splits the detected text into blocks of frames with similar text then picks
	a representative text from each block by choosing the one whose total
	Levenshtein distance from other text in the block is minimal
	"""
	incsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_TEXT_CSV_FILE_NAME
	with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
		reader = csv.reader(incsvfile)
		
		header = next(reader) # Header is ["Frame Index", "Timestamp", "OCR Text"]
		rows = [row for row in reader]
		blocks = [[]]
		current_block = blocks[0]
		for i in range(len(rows)):
			row = rows[i]
			text = row[2]
			start = max(i - window_width, 0)
			best_rel_dist = 1.0
			best_comp_text = ""
			for j in range(start, i):
				comp_text = rows[j][2]
				dist = levenshtein_dist(text, comp_text)
				rel_dist = dist / max(len(text), len(comp_text))
				if rel_dist < best_rel_dist:
					best_rel_dist = rel_dist
					best_comp_text = comp_text
			if best_rel_dist > threshold:
				blocks.append([(row[0], row[1], text)])
				current_block = blocks[-1]
			else:
				current_block.append((row[0], row[1], text))
			video_runner_obj["logger"].info(f"Building block table ... {100.0*float(i)/len(rows)}% complete")
			print('\rBuilding block table ... {:.3}% complete     '.format(100.0*float(i)/len(rows)), end='')
		print('\rBuilding block table ... 100% complete     ')
		video_runner_obj["logger"].info(f"Building block table ... 100% complete")
		filtered_rows = []
		for block in blocks:
			weights = []
			for (frame_index, timestamp, text) in block:
				weight = 0.0
				for (f_i, ts, comp_text) in block:
					dist = levenshtein_dist(text, comp_text)
					rel_dist = dist / max(len(text), len(comp_text))
					weight += rel_dist
				weights.append((frame_index, timestamp, text, weight))
			best_weight = float('inf')
			best_ocr = None
			for (frame_index, timestamp, text, weight) in weights:
				#print(weight, end=' ')
				if weight < best_weight:
					best_weight = weight
					best_ocr = [frame_index, timestamp, text]
			
			if best_ocr:
				filtered_rows.append(best_ocr)
				#print(best_ocr)
		
		outcsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_FILTER_CSV_FILE_NAME
		with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
			writer = csv.writer(outcsvfile)
			writer.writerow([OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])
			for row in filtered_rows:
				writer.writerow(row)
				outcsvfile.flush()

@timeit
def filter_ocr_agreement(video_runner_obj, window_width=10, threshold=0.5, low_threshold=0.05, min_stable_len=5):
	"""
	Splits the detected text into blocks of frames with similar text then picks
	a representative text from each block that has a sufficiently long run of
	very similar texts by choosing the one whose total Levenshtein distance from
	other text in the similar subset of the block is minimal
	"""
	incsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_TEXT_CSV_FILE_NAME
	with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
		reader = csv.reader(incsvfile)
		header = next(reader) # Header is ["Frame Index", "Timestamp", "OCR Text"]
		rows = [row for row in reader]
		blocks = [[]]
		current_block = blocks[0]
		for i in range(len(rows)):
			row = rows[i]
			text = row[2]
			start = max(i - window_width, 0)
			best_rel_dist = 1.0
			best_comp_text = ""
			for j in range(start, i):
				comp_text = rows[j][2]
				dist = levenshtein_dist(text, comp_text)
				rel_dist = dist / max(len(text), len(comp_text))
				if rel_dist < best_rel_dist:
					best_rel_dist = rel_dist
					best_comp_text = comp_text
			if best_rel_dist > threshold:
				blocks.append([(row[0], row[1], best_rel_dist, text)])
				current_block = blocks[-1]
			else:
				current_block.append((row[0], row[1], best_rel_dist, text))
			print('\rBuilding block table ... {:.3}% complete     '.format(100.0*float(i)/len(rows)), end='')
			video_runner_obj["logger"].info(f"Building block table ... {100.0*float(i)/len(rows)}% complete")
		print('\rBuilding block table ... 100% complete     ')
		video_runner_obj["logger"].info(f"Building block table ... 100% complete")
		filtered_rows = []
		for block in blocks:
			low_diff_set = []
			for (frame_index, timestamp, best_rel_dist, text) in block:
				if best_rel_dist < low_threshold:
					low_diff_set.append((frame_index, timestamp, best_rel_dist, text))
			if len(low_diff_set) >= min_stable_len:
				best_diff = float('inf')
				best_ocr = None
				for (frame_index, timestamp, best_rel_dist, text) in low_diff_set:
					if best_rel_dist < best_diff:
						best_diff = best_rel_dist
						best_ocr = [frame_index, timestamp, text]
				if best_ocr:
					filtered_rows.append(best_ocr)
					#print(best_ocr)
		
		outcsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_FILTER_CSV_2_FILE_NAME
		with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
			writer = csv.writer(outcsvfile)
			writer.writerow([OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])
			for row in filtered_rows:
				writer.writerow(row)
				outcsvfile.flush()

def text_difference(source, target):
	"""
	Returns the Levenshtein distance between the source and target strings
	divided by the maximum of their lengths
	"""
	maxlen = max(len(source), len(target))
	return levenshtein_dist(source, target) / maxlen if maxlen > 0 else 0

def remove_non_ascii(source):
	"""
	Removes all characters from the source string that are not ASCII characters
	"""
	return "".join(c for c in source if ord(c) < 128)

@timeit
def filter_ocr_remove_similarity(video_runner_obj, threshold=0.15, use_agreement=True, max_similar_lines=3):
	"""
	Removes non-ASCII characters from all chosen texts and also removes any line
	of text after it has occurred max_similar_lines times
	use_agreement should be set to true if using the results from filter_ocr_agreement
	"""
	if use_agreement:
		incsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_FILTER_CSV_2_FILE_NAME
	else:
		incsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_FILTER_CSV_FILE_NAME
		
	with open(incsvpath, 'r', newline='', encoding='utf-8') as incsvfile:
		reader = csv.reader(incsvfile)
		header = next(reader) # Header is ["Frame Index", "Timestamp", "OCR Text"]
		rows = [row for row in reader]
		kept_rows = []
		for i in range(len(rows)):
			row = rows[i]
			text = row[2]
			keep = True
			smallest_dist = 1.0
			closest_text = None
			for kept_row in kept_rows:
				kept_text = kept_row[2]
				diff = text_difference(text, kept_text)
				if diff < threshold:
					#print("'{}' and '{}' have difference {}\n".format(text, kept_text, diff));
					keep = False
					break
				if diff < smallest_dist:
					smallest_dist = diff
					closest_text = kept_text
			if keep:
				#print("'{}' and '{}' have difference {} - Kept\n".format(text, closest_text, smallest_dist));
				kept_rows.append(row)
			# print('\rComparing text for similarity ... {:.3}% complete     '.format(100.0*float(i)/len(rows)), end='')
		# print('\rComparing text for similarity ... 100% complete     ')
		
		kept_lines = [row[2].split('\n') for row in kept_rows]
		
		for i in range(len(kept_rows)):
			lines = kept_lines[i]
			toRemove = set()
			for line_idx in range(len(lines)):
				line = lines[line_idx]
				similar_line_count = 0
				for j in range(i):
					for prev_line in kept_lines[j]:
						diff = text_difference(line, prev_line)
						if diff < threshold:
							similar_line_count += 1
							break
				if similar_line_count > max_similar_lines:
					#print("Removed: " + line)
					toRemove.add(line_idx)
			remaining_lines = [lines[idx] for idx in range(len(lines)) if idx not in toRemove]
			kept_rows[i][2] = remove_non_ascii('\n'.join(remaining_lines))
		
		outcsvpath = return_video_folder_name(video_runner_obj)+ "/"+ OCR_FILTER_REMOVE_SIMILAR
		with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
			writer = csv.writer(outcsvfile)
			writer.writerow([OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])
			for row in kept_rows:
				writer.writerow(row)
				outcsvfile.flush()


			#print(r)

	


if __name__ == "__main__":
	# video_name = 'A dog collapses and faints right in front of us I have never seen anything like it'
	# video_name = 'Good Samaritans knew that this puppy needed extra help'
	# video_name = 'Hope For Paws Stray dog walks into a yard and then collapses'
	# video_name = 'This rescue was amazing - Im so happy I caught it on camera!!!'
	# video_name = 'Oh wow this rescue turned to be INTENSE as the dog was fighting for her life!!!'
	# video_name = 'Hope For Paws_ A homeless dog living in a trash pile gets rescued, and then does something amazing!'
	video_name = 'Homeless German Shepherd cries like a human!  I have never heard anything like this!!!'
	# video_id = sys.argv[1]
	filter_ocr(video_name)
	filter_ocr_agreement(video_name)
	filter_ocr_remove_similarity(video_name)
