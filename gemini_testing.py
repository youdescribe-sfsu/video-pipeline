import os
import base64
from dotenv import load_dotenv
import google.generativeai as genai
from pipeline_module.utils_module.utils import (
    return_video_folder_name
)

# Load environment variables
load_dotenv()

# Configure Generative AI API
gemini_api_key = os.getenv("GEMINI_API_KEY")
if not gemini_api_key:
    raise ValueError("GEMINI_API_KEY is not set in the environment.")
genai.configure(api_key=gemini_api_key)

# Define the generative model
model = genai.GenerativeModel("gemini-1.5-flash")

def generate_gemini_content(prompt: str) -> str:
    """
    Generates content using the Gemini API.
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error generating Gemini content: {e}")
        return ""

# Define system prompt
system_prompt = (
    "Generate a unique, distinct, and concise description for a video segment. "
    "The description should be tailored to be accessible for blind and visually impaired individuals following these guidelines. \n\n"
    "Guidelines - a Quality Description Must Be:\n"
    "Accurate, Prioritized, Consistent, Appropriate, Clarity, and Equal.\n\n"
    "The 8 frames are provided as a single image where frames are read from left to right. "
    "Please make the description representative of the whole video clip and not frame by frame."
)

def process_video_frames(video_folder_path):
    """
    Process video frames and generate captions for each frame.
    """
    try:
        # Paths and setup
        video_frames_path = os.path.join(video_folder_path, "collages")
        
        # Read frame data
        num_frames = len(os.listdir(video_frames_path))

        # Process each frame
        for i in range(5):

            # Dynamically retrieve file names in the directory
            frame_filenames = sorted(
                [os.path.join(video_frames_path, file) for file in os.listdir(video_frames_path)]
            )

            # Ensure the frame index does not exceed the number of files
            if i < len(frame_filenames):
                frame_filename = frame_filenames[i]
            else:
                self.logger.warning(f"Frame {i} does not exist in {video_frames_path}, skipping.")
                continue

            print(f"Processing frame: {frame_filename}")

            # Generate caption
            caption = get_gemini_caption(frame_filename, system_prompt)
            print(f"Caption for {frame_filename}: {caption}")

    except Exception as e:
        print(f"Error processing video frames: {e}")

def get_gemini_caption(filename: str, system_prompt: str) -> str:
    """
    Generate a caption for a given filename using the Gemini API.
    """
    try:
        with open(filename, "rb") as file:
            image_base64 = base64.b64encode(file.read()).decode('utf-8')
        response = model.generate_content([{'mime_type':'image/jpeg', 'data': image_base64}, system_prompt])
        return response.text
    except Exception as e:
        print(f"Error generating Gemini caption: {e}")
        return ""

# Example usage
video_folder_path = "/home/wang.yu-e/datasets/aiAudioDescriptionDataset-prod/z_XoR2ow-74_files_650506db3ff1c2140ea10ece"
process_video_frames(video_folder_path)
