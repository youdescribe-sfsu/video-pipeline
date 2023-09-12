from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import read_value_from_file, return_video_download_location,  return_video_frames_folder, save_value_to_file
import cv2
import os
from typing import Dict
from concurrent.futures import ThreadPoolExecutor
# import numba

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, int], frames_per_second: int):
        self.video_runner_obj = video_runner_obj
        self.frames_per_second = frames_per_second
        self.logger = video_runner_obj.get("logger")

    def extract_frames(self, logging=False):
        vid_name = return_video_frames_folder(self.video_runner_obj)
        
        if(read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['started']") == 'done'):
            self.logger.info("Frames already extracted, skipping step.")
            return

        if not os.path.exists(vid_name):
            try:
                os.mkdir(vid_name)
            except OSError:
                self.logger.error("Cannot create directory for frames")
                return
        video_location = return_video_download_location(self.video_runner_obj)
        vid = cv2.VideoCapture(video_location)
            
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['started']"):
            fps = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['fps']")
            frames_per_extraction = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['frames_per_extraction']")
            num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['num_frames']")
            actual_frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['actual_frames_per_second']")
        else:
            fps = round(vid.get(cv2.CAP_PROP_FPS))
            frames_per_extraction = round(fps / self.frames_per_second)
            num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
            actual_frames_per_second = fps / frames_per_extraction
            
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value=True)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['fps']", value=fps)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['frames_per_extraction']", value=frames_per_extraction)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['num_frames']", value=num_frames)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['actual_frames_per_second']", value=actual_frames_per_second)

        last_extracted_frame = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['extract_frames']")
        if last_extracted_frame >= num_frames:
            self.logger.info("Frames already extracted, skipping step.")
            return

        self.logger.info(f"Extracting frames from {self.video_runner_obj['video_id']} ({fps} fps, {num_frames} frames)...")

        frame_count = last_extracted_frame
        while frame_count < num_frames:
            status, frame = vid.read()
            if not status:
                self.logger.error("Error extracting frame {}.".format(frame_count))
                break
            if frame_count % frames_per_extraction == 0:
                frame_filename = "{}/frame_{}.jpg".format(vid_name, frame_count)
                cv2.imwrite(frame_filename, frame)
            self.logger.info(f"{frame_count * 100 // num_frames}% complete")
            self.logger.info("\r{}% complete  ".format((frame_count * 100) // num_frames))
            frame_count += 1
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=frame_count)

        # if logging:
        self.logger.info("\r100% complete   ")

        vid.release()
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']", value=frames_per_extraction)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['num_frames']", value=num_frames)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['frames_per_second']", value=actual_frames_per_second)
        
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=frame_count)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value='done')

        self.logger.info("Extraction complete.")
        return
    
    
    def extract_frames_parallel(self, num_workers=4):
        vid_name = return_video_frames_folder(self.video_runner_obj)

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']") == 'done':
            self.logger.info("Frames already extracted, skipping step.")
            return

        if not os.path.exists(vid_name):
            try:
                os.mkdir(vid_name)
            except OSError:
                self.logger.error("Cannot create directory for frames")
                return

        video_location = return_video_download_location(self.video_runner_obj)
        vid = cv2.VideoCapture(video_location)
        fps = round(vid.get(cv2.CAP_PROP_FPS))
        frames_per_extraction = round(fps / self.frames_per_second)
        num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
        actual_frames_per_second = fps / frames_per_extraction

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value=True)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['fps']", value=fps)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['frames_per_extraction']", value=frames_per_extraction)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['num_frames']", value=num_frames)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['actual_frames_per_second']", value=actual_frames_per_second)

        last_extracted_frame = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']")
        if last_extracted_frame >= num_frames:
            self.logger.info("Frames already extracted, skipping step.")
            return

        self.logger.info(f"Extracting frames from {self.video_runner_obj['video_id']} ({fps} fps, {num_frames} frames)...")

        # @numba.njit
        def process_frame(frame_count):
            status, frame = vid.read()
            if not status:
                return
            if frame_count % frames_per_extraction == 0:
                frame_filename = "{}/frame_{}.jpg".format(vid_name, frame_count)
                cv2.imwrite(frame_filename, frame)
                self.logger.info(f"{frame_count * 100 // num_frames}% complete")
                self.logger.info("\r{}% complete  ".format((frame_count * 100) // num_frames))
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=frame_count)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            frame_indices = range(last_extracted_frame, num_frames, frames_per_extraction)
            executor.map(process_frame, frame_indices)

        self.logger.info("\r100% complete   ")

        vid.release()
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']", value=frames_per_extraction)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['num_frames']", value=num_frames)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['frames_per_second']", value=actual_frames_per_second)

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=num_frames)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value='done')

        self.logger.info("Extraction complete.")
        return
