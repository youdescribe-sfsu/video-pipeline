from typing import Optional
from pydantic import BaseModel

class VideoCommonValues(BaseModel):
    step: Optional[str]
    num_frames: Optional[int]
    frames_per_second: Optional[float]

class ImportVideoProgress(BaseModel):
    download_video: int

class ExtractAudioProgress(BaseModel):
    extract_audio: int

class SpeechToTextProgress(BaseModel):
    upload_blob: int
    getting_speech_from_audio: int
    delete_blob: int

class FrameExtractionProgress(BaseModel):
    started: bool
    frame_extraction_rate: int
    extract_frames: int
    num_frames: int

class OCRProgress(BaseModel):
    started: bool
    detect_watermark: int
    get_all_ocr: int
    filter_ocr: int
    filter_ocr_agreement: int
    filter_ocr_remove_similarity: int

class ObjectDetectionProgress(BaseModel):
    started: bool
    step: int
    num_frames: int

class KeyframeSelectionProgress(BaseModel):
    started: bool

class RunImageCaptioningProgress(BaseModel):
    started: bool
    last_processed_frame: int

class ImageCaptioningProgress(BaseModel):
    started: bool
    run_image_captioning: RunImageCaptioningProgress
    combine_image_caption: int

class CaptionRatingProgress(BaseModel):
    started: bool
    last_processed_frame: int
    get_all_caption_rating: int
    filter_captions: int

class SceneSegmentationProgress(BaseModel):
    started: bool
    generate_average_output: int
    run_scene_segmentation: int

class TextSummarizationProgress(BaseModel):
    started: bool

class UploadToYDXProgress(BaseModel):
    started: bool
    generateYDXCaption: int

class DefaultSaveProgress(BaseModel):
    video_id: str
    video_common_values: VideoCommonValues
    ImportVideo: ImportVideoProgress
    ExtractAudio: ExtractAudioProgress
    SpeechToText: SpeechToTextProgress
    FrameExtraction: FrameExtractionProgress
    OCR: OCRProgress
    ObjectDetection: ObjectDetectionProgress
    KeyframeSelection: KeyframeSelectionProgress
    ImageCaptioning: ImageCaptioningProgress
    CaptionRating: CaptionRatingProgress
    SceneSegmentation: SceneSegmentationProgress
    TextSummarization: TextSummarizationProgress
    UploadToYDX: UploadToYDXProgress
