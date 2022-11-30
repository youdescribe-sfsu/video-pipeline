from csv_generate import generateOutputAvg
from sceneSegmentation import sceneSegmentation
from text_summarization import text_summarization_csv

class TextSummary:
    def __init__(self, video_id):
        self.video_id = video_id
    
    
    def generateTextSummary(self):
        generateOutputAvg(self.video_id)
        sceneSegmentation(self.video_id)
        text_summarization_csv(self.video_id)
