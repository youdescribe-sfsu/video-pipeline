# caption_rating.py
import csv
import json
import traceback
from typing import Dict, Any, List
import requests
import aiohttp
import asyncio
import aiofiles
from datetime import datetime
from web_server_module.web_server_database import (
    get_status_for_youtube_id,
    update_status,
    update_module_output,
    get_module_output
)
from ..utils_module.utils import (
    return_video_folder_name,
    CAPTION_SCORE,
    OBJECTS_CSV,
    CAPTIONS_CSV,
    CAPTIONS_AND_OBJECTS_CSV,
    CAPTION_IMAGE_PAIR
)


class CaptionRating:
    """Enhanced caption rating service with explicit service URL management"""

    def __init__(
            self,
            video_runner_obj: Dict[str, Any],
            service_url: Optional[str] = None,
            rating_threshold: float = 0.5
    ):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from video_runner_obj if available, fallback to parameter
        self.service_url = service_url or video_runner_obj.get("rating_url")
        if not self.service_url:
            raise ValueError("Rating service URL must be provided")

        self.rating_threshold = rating_threshold
        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'

    async def perform_caption_rating(self) -> bool:
        """Main entry point for caption rating process"""
        try:
            if get_status_for_youtube_id(
                    self.video_runner_obj["video_id"],
                    self.video_runner_obj["AI_USER_ID"]
            ) == "done":
                self.logger.info("Caption rating already processed")
                return True

            await self.process_all_captions()
            await self.filter_captions()

            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error in perform_caption_rating: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def get_caption_rating(self, image_data: Dict[str, str]) -> str:
        """Get rating for a single caption with async request"""
        payload = {
            'token': self.token,
            'img_url': image_data['frame_url'],
            'caption': image_data['caption']
        }

        start_time = datetime.now()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.service_url, data=payload) as response:
                    if response.status != 200:
                        raise requests.RequestException(
                            f"Rating service returned status {response.status}"
                        )

                    text = await response.text()
                    rating = text.lstrip("['").rstrip("']")

                    elapsed = (datetime.now() - start_time).total_seconds()
                    self.logger.info(
                        f"Caption rating request completed in {elapsed:.2f}s - "
                        f"Rating: {rating}"
                    )

                    return rating

        except aiohttp.ClientError as e:
            self.logger.error(f"Error in caption rating request: {str(e)}")
            return "0.0"
        except ValueError as e:
            self.logger.error(f"Invalid rating value received: {text}")
            return "0.0"

    async def process_all_captions(self) -> None:
        """Process all captions with async handling"""
        image_caption_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTION_IMAGE_PAIR
        )
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTION_SCORE
        )

        # Get processed frames from database
        processed_frames = await self.get_processed_frames()

        # Process new frames
        async with aiofiles.open(image_caption_file, 'r', encoding='utf-8') as infile:
            data = list(csv.DictReader(await infile.read().splitlines()))

        # Process captions in parallel
        tasks = []
        for image_data in data:
            frame_index = int(image_data['frame_index'])
            if frame_index not in processed_frames:
                tasks.append(self.process_single_caption(image_data))

        results = await asyncio.gather(*tasks)

        # Save results
        async with aiofiles.open(output_file, 'a', newline='') as outfile:
            writer = csv.writer(outfile)
            if not os.path.exists(output_file):  # Write header for new file
                await writer.writerow(['frame_index', 'frame_url', 'caption', 'rating'])

            for result in results:
                if result:  # Only write valid results
                    await writer.writerow([
                        result['frame_index'],
                        result['frame_url'],
                        result['caption'],
                        result['rating']
                    ])

        # Update database with processed frames
        await self.update_processed_frames([r['frame_index'] for r in results if r])

    async def process_single_caption(self, image_data: Dict[str, str]) -> Optional[Dict]:
        """Process a single caption and return result"""
        try:
            rating = await self.get_caption_rating(image_data)
            self.logger.info(
                f"Rating for caption '{image_data['caption']}' is {rating}"
            )

            return {
                'frame_index': image_data['frame_index'],
                'frame_url': image_data['frame_url'],
                'caption': image_data['caption'],
                'rating': rating
            }
        except Exception as e:
            self.logger.error(f"Error processing caption: {str(e)}")
            return None

    async def filter_captions(self) -> None:
        """Filter captions based on ratings"""
        caption_filter_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTION_SCORE
        )
        objects_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            OBJECTS_CSV
        )
        captions_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTIONS_CSV
        )
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTIONS_AND_OBJECTS_CSV
        )

        # Get filtered frame indices
        async with aiofiles.open(caption_filter_file, 'r') as filter_file:
            reader = csv.DictReader(await filter_file.read().splitlines())
            filtered_frames = {
                row['frame_index']
                for row in reader
                if float(row['rating']) > self.rating_threshold
            }

        # Combine filtered captions with objects
        async with aiofiles.open(objects_file, 'r') as objfile, \
                aiofiles.open(captions_file, 'r') as captfile, \
                aiofiles.open(output_file, 'w', newline='') as outfile:

            obj_content = await objfile.read()
            capt_content = await captfile.read()

            obj_reader = csv.reader(obj_content.splitlines())
            capt_reader = csv.reader(capt_content.splitlines())
            writer = csv.writer(outfile)

            # Write headers
            obj_header = next(obj_reader)
            capt_header = next(capt_reader)
            await writer.writerow(capt_header + obj_header[1:])

            # Combine rows for filtered frames
            for capt_row in capt_reader:
                if capt_row[0] in filtered_frames:
                    obj_row = next(obj_reader)
                    await writer.writerow(capt_row + obj_row[1:])

        self.logger.info(
            f"Caption filtering complete for {self.video_runner_obj['video_id']}"
        )

    async def get_processed_frames(self) -> List[int]:
        """Get list of already processed frame indices"""
        try:
            module_output = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating'
            )
            return module_output.get('processed_frames', [])
        except Exception as e:
            self.logger.error(f"Error getting processed frames: {str(e)}")
            return []

    async def update_processed_frames(self, new_frames: List[int]) -> None:
        """Update list of processed frames in database"""
        try:
            current_frames = await self.get_processed_frames()
            updated_frames = list(set(current_frames + new_frames))

            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating',
                {'processed_frames': updated_frames}
            )
        except Exception as e:
            self.logger.error(f"Error updating processed frames: {str(e)}")