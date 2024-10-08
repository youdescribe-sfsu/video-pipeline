from .text_summarization_helper import TextSummarization
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import return_video_folder_name


class TextSummaryCoordinator:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

    def generate_text_summary(self):
        # Check if the summarization is already done
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("Text summarization already processed")
            return True

        try:
            self.logger.info("Starting text summarization process")

            # Initialize TextSummarization from text_summarization_helper.py
            text_summarization = TextSummarization(self.video_runner_obj)

            # Generate the text summary
            text_summarization.generate_text_summary()

            # Mark the task as done
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("Text summarization process completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in text summarization process: {str(e)}")
            return False

if __name__ == "__main__":
    # Testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Using print as a simple logger for testing
    }
    coordinator = TextSummaryCoordinator(video_runner_obj)
    coordinator.generate_text_summary()