import csv
import warnings
import numpy as np
import os
from typing import Dict, Union, Optional, Any
from web_server_module.web_server_database import (
    update_status,
    get_status_for_youtube_id,
    update_module_output
)
from ..utils_module.utils import (
    CAPTIONS_AND_OBJECTS_CSV,
    OUTPUT_AVG_CSV,
    return_video_folder_name,
    return_int_if_possible
)

# Enable warnings to catch numerical computation issues
warnings.filterwarnings("error")


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> Union[float, str]:
    """
    Compute cosine similarity between two vectors, with robust error handling.
    Returns "NaN" string if computation fails.
    """
    try:
        return np.dot(v1, v2) / (np.linalg.norm(v1, ord=2) * np.linalg.norm(v2, ord=2))
    except (RuntimeWarning, ZeroDivisionError):
        return "NaN"


def validate_numerical_columns(row: Dict[str, str], numerical_columns: List[str]) -> bool:
    """
    Validate that specified columns contain valid numerical data.
    Returns False if any numerical column contains invalid data.
    """
    for col in numerical_columns:
        if row[col] and row[col] != '':
            try:
                float(row[col])
            except ValueError:
                return False
    return True


def generate_average_output(video_runner_obj: Dict[str, Any]) -> bool:
    """
    Generate output average CSV file for a video with improved error handling
    and data validation.
    """
    video_id = video_runner_obj["video_id"]
    logger = video_runner_obj.get("logger")

    try:
        # Check if average output generation is already completed
        if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
            logger.info(f"Average output already generated for video {video_id}, skipping step.")
            return True

        # Define paths for input and output files
        captions_and_objects_csv = os.path.join(
            return_video_folder_name(video_runner_obj),
            CAPTIONS_AND_OBJECTS_CSV
        )
        output_avg_csv = os.path.join(
            return_video_folder_name(video_runner_obj),
            OUTPUT_AVG_CSV
        )

        # Check if input CSV exists
        if not os.path.exists(captions_and_objects_csv):
            logger.error(f"Required input file not found: {captions_and_objects_csv}")
            return False

        # Initialize data structures
        isKeyFrame = []
        description = []
        frame_index = []
        timestamp = []
        vector_list = []

        # Define metadata and numerical columns
        metadata_columns = ['frame_index', 'frame_url', 'caption', 'rating', 'timestamp']

        # Read and process CSV data
        with open(captions_and_objects_csv, 'r') as csvFile:
            csvReader = csv.DictReader(csvFile)

            # Get numerical columns (everything not in metadata)
            numerical_columns = [col for col in csvReader.fieldnames if col not in metadata_columns]

            # Process each row
            for row in csvReader:
                # Validate numerical data
                if not validate_numerical_columns(row, numerical_columns):
                    logger.error(f"Invalid numerical data found in row: {row['frame_index']}")
                    continue

                # Extract metadata
                frame_index.append(return_int_if_possible(float(row['frame_index'])))
                timestamp.append(return_int_if_possible(float(row['timestamp'])))
                isKeyFrame.append(row.get('is_keyframe', 'False'))
                description.append(row.get('caption', ''))

                # Process numerical columns
                numerical_values = []
                for col in numerical_columns:
                    value = row.get(col, '')
                    numerical_values.append(float(value) if value != '' else 0.0)
                vector_list.append(numerical_values)

        # Generate similarity data
        data = []
        for idx in range(2, len(vector_list) - 1):
            # Calculate similarity with next frame
            similarity = return_int_if_possible(
                cosine_similarity(vector_list[idx], vector_list[idx + 1])
            )

            # Calculate average similarities if possible
            avg_one = avg_two = 0.0
            if idx < len(vector_list) - 3:
                avg_one = return_int_if_possible(
                    cosine_similarity(vector_list[idx - 1], vector_list[idx + 2])
                )
                avg_two = return_int_if_possible(
                    cosine_similarity(vector_list[idx - 2], vector_list[idx + 3])
                )

            # Convert NaN to SKIP for better downstream processing
            similarity = "SKIP" if similarity == "NaN" else similarity

            # Compile row data
            data.append([
                frame_index[idx],
                timestamp[idx],
                idx,
                idx + 1,
                similarity,
                avg_one,
                avg_two,
                isKeyFrame[idx],
                description[idx]
            ])

        # Write output CSV
        with open(output_avg_csv, 'w', newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow([
                'frame',
                'timestamp',
                'Line1',
                'Line2',
                'Similarity',
                'avgone',
                'avgtwo',
                'iskeyFrame',
                'description'
            ])
            writer.writerows(data)

        logger.info(f"Output avg CSV file generated for video: {video_id}")

        # Update database with results
        update_module_output(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            'generate_average_output',
            {"average_output": data}
        )

        # Mark task as done
        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")

        return True

    except Exception as e:
        logger.error(f"Error in generating average output for video {video_id}: {str(e)}")
        return False