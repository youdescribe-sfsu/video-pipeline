from text_summarization_module.text_summarization_helper import text_summarization_csv

class TextSummarization:
    def __init__(self, video_id):
        self.video_id = video_id
    
    
    def generate_text_summary(self):
        text_summarization_csv(self.video_id)
