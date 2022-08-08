
import io
import os
import csv

def insertScenes(video_name):
	"""
	Inserts the scenes into the DB
	"""
	with open('{}-scenes.csv'.format(video_name), newline='', encoding='utf-8') as incsvkeyframes:
            reader = csv.reader(incsvkeyframes)
            rows = [row for row in reader]
            header = next(reader)
            for row in rows:
                print(row)

                
if __name__ == "__main__":
	video_name = "aACZE55svEl"
	insertScenes(video_name)


