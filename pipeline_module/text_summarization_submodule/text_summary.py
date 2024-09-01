from .text_summarization_helper import generate_text_summary
from ..utils_module.utils import load_progress_from_file, read_value_from_file, save_progress_to_file, save_value_to_file

class TextSummarization:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
    
    
    def generate_text_summary(self):
        # save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        # if(save_file['TextSummarization']['started'] == 'done'):
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['TextSummarization']['started']") == 'done':
            ## Already processed
            print("Already processed")
            self.video_runner_obj["logger"].info("Already processed")
            return
        generate_text_summary(self.video_runner_obj)
        # save_file['TextSummarization']['started'] = 'done'
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['TextSummarization']['started']", value='done')
        print("TextSummarization done")
        return
