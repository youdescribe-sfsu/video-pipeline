import os
from typing import Dict, Any, List, Optional, Tuple
import numpy as np


def validate_timestamp(timestamp: float) -> bool:
    """
    Validate that a timestamp is reasonable.
    """
    return isinstance(timestamp, (int, float)) and timestamp >= 0


def clean_description(description: str) -> str:
    """
    Clean and normalize scene descriptions.
    """
    if not description:
        return ""

    # Remove extra whitespace
    description = ' '.join(description.split())
    # Remove leading/trailing newlines
    description = description.strip('\n')
    return description


def merge_overlapping_scenes(scenes: List[Dict[str, Any]],
                             overlap_threshold: float = 2.0) -> List[Dict[str, Any]]:
    """
    Merge scenes that overlap significantly in time.
    Args:
        scenes: List of scene dictionaries with start_time, end_time, text
        overlap_threshold: Maximum seconds of overlap before merging
    Returns:
        List of merged scenes
    """
    if not scenes:
        return []

    # Sort scenes by start time
    sorted_scenes = sorted(scenes, key=lambda x: x['start_time'])
    merged = []
    current = sorted_scenes[0]

    for next_scene in sorted_scenes[1:]:
        if next_scene['start_time'] - current['end_time'] < -overlap_threshold:
            # Merge overlapping scenes
            current['end_time'] = max(current['end_time'], next_scene['end_time'])
            current['text'] = f"{current['text']}\n{next_scene['text']}"
        else:
            merged.append(current)
            current = next_scene

    merged.append(current)
    return merged


def validate_scene_boundaries(scenes: List[Dict[str, Any]],
                              video_duration: float) -> List[Dict[str, Any]]:
    """
    Validate and fix scene boundaries to ensure they make sense.
    Args:
        scenes: List of scene dictionaries
        video_duration: Total video duration in seconds
    Returns:
        List of validated scenes with fixed boundaries
    """
    if not scenes:
        return []

    validated = []
    current_time = 0.0

    for scene in scenes:
        if not validate_timestamp(scene.get('start_time')) or \
                not validate_timestamp(scene.get('end_time')):
            continue

        # Ensure scene starts after previous scene
        start = max(scene['start_time'], current_time)
        end = min(scene['end_time'], video_duration)

        if end > start:
            validated.append({
                'start_time': start,
                'end_time': end,
                'text': clean_description(scene.get('text', ''))
            })
            current_time = end

    return validated


def calculate_scene_similarity(scene1: Dict[str, Any],
                               scene2: Dict[str, Any]) -> float:
    """
    Calculate similarity between two scenes based on their descriptions.
    Uses a simple bag-of-words cosine similarity.
    """
    import re
    from collections import Counter

    def tokenize(text: str) -> List[str]:
        return re.findall(r'\w+', text.lower())

    tokens1 = Counter(tokenize(scene1.get('text', '')))
    tokens2 = Counter(tokenize(scene2.get('text', '')))

    if not tokens1 or not tokens2:
        return 0.0

    intersection = sum((tokens1 & tokens2).values())
    norm1 = np.sqrt(sum(x * x for x in tokens1.values()))
    norm2 = np.sqrt(sum(x * x for x in tokens2.values()))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return intersection / (norm1 * norm2)


def filter_redundant_scenes(scenes: List[Dict[str, Any]],
                            similarity_threshold: float = 0.8) -> List[Dict[str, Any]]:
    """
    Filter out redundant scenes based on content similarity.
    """
    if not scenes:
        return []

    filtered = [scenes[0]]

    for scene in scenes[1:]:
        # Check similarity with previous scenes
        similarities = [calculate_scene_similarity(scene, prev)
                        for prev in filtered[-3:]]  # Compare with last 3 scenes

        if not similarities or max(similarities) < similarity_threshold:
            filtered.append(scene)

    return filtered