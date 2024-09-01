from .text_summarization_helper import TextSummarization
from ..utils_module.utils import read_value_from_file, save_value_to_file

class TextSummaryCoordinator:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
    
    def generate_text_summary(self):
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['TextSummarization']['started']") == 'done':
            self.logger.info("Text summarization already processed")
            return

        try:
            self.logger.info("Starting text summarization process")
            
            # Initialize TextSummarization from text_summarization_helper.py
            text_summarization = TextSummarization(self.video_runner_obj)
            
            # Generate the text summary
            text_summarization.generate_text_summary()
            
            # Mark the process as complete
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['TextSummarization']['started']", value='done')
            
            self.logger.info("Text summarization process completed successfully")
        
        except Exception as e:
            self.logger.error(f"Error in text summarization process: {str(e)}")
            raise

if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    coordinator = TextSummaryCoordinator(video_runner_obj)
    coordinator.generate_text_summary()
