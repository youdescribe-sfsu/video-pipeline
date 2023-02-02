from utils import returnVideoDownloadLocation
import ffmpeg


class ExtractAudio:
    def __init__(self, video_id):
        """
        Initialize ExtractAudio object
        
        Parameters:
        video_id (str): YouTube video ID
        """
        self.video_id = video_id
    
    def extract_audio(self):
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.
        """
        # Define the input and output file paths
        input_file = returnVideoDownloadLocation(self.video_id)
        output_file = input_file.replace(".mp4", ".flac")
        # Use ffmpeg to extract the audio and save it as a FLAC file
        ffmpeg.input(input_file).output(output_file).run()
        
        return
