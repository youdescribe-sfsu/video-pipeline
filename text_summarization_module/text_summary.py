from text_summarization_module.text_summarization_helper import text_summarization_csv
from utils import load_progress_from_file, save_progress_to_file

class TextSummarization:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
    
    
    def generate_text_summary(self):
        save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        if(save_file['TextSummarization']['started'] == 'done'):
            ## Already processed
            self.video_runner_obj["logger"].info("Already processed")
            return
        text_summarization_csv(self.video_runner_obj)
        save_file['TextSummarization']['started'] = 'done'
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        return
