from text_summarization_module.text_summarization_helper import text_summarization_csv

class TextSummarization:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
    
    
    def generate_text_summary(self):
        text_summarization_csv(self.video_runner_obj)
