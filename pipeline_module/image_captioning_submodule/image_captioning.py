# image_captioning.py
import csv
import os
import traceback
from typing import Dict, Any, List, Optional
import aiohttp
import asyncio
import aiofiles
from datetime import datetime
from web_server_module.web_server_database import (
    update_status,
    update_module_output,
    get_module_output
)
from ..utils_module.utils import (
    CAPTIONS_CSV,
    FRAME_INDEX_SELECTOR,
    IS_KEYFRAME_SELECTOR,
    KEY_FRAME_HEADERS,
    KEYFRAME_CAPTION_SELECTOR,
    KEYFRAMES_CSV,
    TIMESTAMP_SELECTOR,
    return_video_folder_name,
    return_video_frames_folder,
    CAPTION_IMAGE_PAIR
)
from ..utils_module.timeit_decorator import timeit


class ImageCaptioning:
    """Enhanced image captioning with service URL injection"""

    def __init__(
            self,
            video_runner_obj: Dict[str, Any],
            service_url: Optional[str] = None,
            min_length: int = 25,
            max_length: int = 50
    ):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from video_runner_obj if available, fallback to parameter
        self.service_url = service_url or video_runner_obj.get("caption_url")
        if not self.service_url:
            raise ValueError("Captioning service URL must be provided")

        self.min_length = min_length
        self.max_length = max_length
        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.logger.info(f"image_captioning initialized with endpoint: {self.service_url}")

    @timeit
    async def run_image_captioning(self) -> bool:
        """Main entry point for image captioning process"""
        try:
            frame_data = await self.get_frame_extraction_data()
            if not frame_data:
                raise ValueError("No frame extraction data found")

            step = int(frame_data['steps'])
            num_frames = int(frame_data['frames_extracted'])
            fps = float(frame_data['adaptive_fps'])

            video_fps = step * fps
            seconds_per_frame = 1.0 / video_fps

            keyframes = await self.load_keyframes()

            # Process frames in parallel
            tasks = []
            frames_path = return_video_frames_folder(self.video_runner_obj)

            for frame_index in range(0, num_frames, step):
                frame_file = f'{frames_path}/frame_{frame_index}.jpg'
                if os.path.exists(frame_file):
                    tasks.append(self.process_single_frame(
                        frame_file,
                        frame_index,
                        seconds_per_frame,
                        frame_index in keyframes
                    ))

            results = await asyncio.gather(*tasks)

            # Save results
            await self.save_captions(results)

            # Update database
            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )

            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'image_captioning',
                {"captions_completed": True}
            )

            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def get_frame_extraction_data(self) -> Dict:
        """Get frame extraction data from database"""
        data = get_module_output(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            'frame_extraction'
        )
        if not data:
            raise ValueError("Frame extraction data not found")
        return data

    async def load_keyframes(self) -> List[int]:
        """Load keyframe indices from file"""
        keyframes_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            KEYFRAMES_CSV
        )
        async with aiofiles.open(keyframes_file, 'r') as infile:
            content = await infile.read()
            reader = csv.reader(content.splitlines())
            next(reader)
            return [int(row[0]) for row in reader]

    async def get_caption(self, filename: str) -> str:
        """Get caption for a single image with async request"""
        async with aiofiles.open(filename, 'rb') as file:
            file_data = await file.read()

        form = aiohttp.FormData()
        form.add_field('token', self.token)
        form.add_field(
            'image',
            file_data,
            filename=os.path.basename(filename)
        )
        form.add_field('min_length', str(self.min_length))
        form.add_field('max_length', str(self.max_length))

        start_time = datetime.now()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.service_url, data=form) as response:
                    if response.status != 200:
                        raise aiohttp.ClientError(
                            f"Caption service returned status {response.status}"
                        )

                    data = await response.json()
                    caption = data['caption']

                    elapsed = (datetime.now() - start_time).total_seconds()
                    self.logger.info(
                        f"Caption request completed in {elapsed:.2f}s - "
                        f"Caption: {caption}"
                    )

                    return caption.strip()

        except Exception as e:
            self.logger.error(f"Error getting caption: {str(e)}")
            return ""

    async def process_single_frame(
            self,
            frame_file: str,
            frame_index: int,
            seconds_per_frame: float,
            is_keyframe: bool
    ) -> Optional[Dict]:
        """Process a single frame and return result"""
        try:
            caption = await self.get_caption(frame_file)
            if caption:
                return {
                    'frame_index': frame_index,
                    'timestamp': frame_index * seconds_per_frame,
                    'is_keyframe': is_keyframe,
                    'caption': caption
                }
            return None
        except Exception as e:
            self.logger.error(f"Error processing frame {frame_index}: {str(e)}")
            return None

    async def save_captions(self, results: List[Optional[Dict]]) -> None:
        """Save caption results to file"""
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTIONS_CSV
        )

        async with aiofiles.open(output_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            await writer.writerow([
                KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],
                KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],
                KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]
            ])

            for result in results:
                if result:  # Only write valid results
                    await writer.writerow([
                        result['frame_index'],
                        result['timestamp'],
                        result['is_keyframe'],
                        result['caption']
                    ])

    async def combine_image_caption(self) -> bool:
        """Combine frame data with captions"""
        try:
            captions_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_CSV
            )

            if not os.path.exists(captions_file):
                raise FileNotFoundError(f"Captions file not found: {captions_file}")

            frames_path = return_video_frames_folder(self.video_runner_obj)
            async with aiofiles.open(captions_file, 'r') as infile:
                reader = csv.DictReader(await infile.read().splitlines())
                pairs = [{
                    "frame_index": row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]],
                    "frame_url": f'{frames_path}/frame_{row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]}.jpg',
                    "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
                } for row in reader]

            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_IMAGE_PAIR
            )

            async with aiofiles.open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                await writer.writerow(['frame_index', 'frame_url', 'caption'])
                for pair in pairs:
                    await writer.writerow([
                        pair['frame_index'],
                        pair['frame_url'],
                        pair['caption']
                    ])

            self.logger.info("Successfully combined image captions")
            return True

        except Exception as e:
            self.logger.error(f"Error combining image captions: {str(e)}")
            return False