import json
import yt_dlp as ydl
from utils import returnVideoDownloadLocation,returnVideoFolderName
from datetime import timedelta
from timeit_decorator import timeit
import ffmpeg
import os

@timeit
def import_video(videoId, video_start_time=None, video_end_time=None):
    """
    Get Audio and Video Simultaneously
    videoId : str : YouTube video ID
    video_start_time : int : Start time of the video (in seconds)
    video_end_time : int : End time of the video (in seconds)
    """
    # Download video from YouTube
    print("Downloading video from YouTube")
    
    ydl_opts = {'outtmpl': returnVideoDownloadLocation(videoId), "format": "best" }
    vid = ydl.YoutubeDL(ydl_opts).extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=True)

    # Get Video Duration
    duration = vid.get('duration')

    # Get Video Title
    title = vid.get('title')
    print("Video Title: ", title)

    # Save metadata to json file
    with open(returnVideoFolderName(videoId) + '/metadata.json', 'w') as f:
        f.write(json.dumps({'duration': duration, 'title': title}))

    if video_start_time and video_end_time:
        # Convert start and end time to timedelta
        start_time = timedelta(seconds=int(video_start_time))
        end_time = timedelta(seconds=int(video_end_time))
        print("start time: ", start_time)
        print("end time: ", end_time)

        # Trim video and audio based on start and end time
        input_stream = ffmpeg.input(returnVideoDownloadLocation(videoId))
        vid = (
            input_stream.video
            .trim(start=video_start_time, end=video_end_time)
            .setpts('PTS-STARTPTS')
        )
        aud = (
            input_stream.audio
            .filter_('atrim', start=video_start_time, end=video_end_time)
            .filter_('asetpts', 'PTS-STARTPTS')
        )

        # Join trimmed video and audio
        joined = ffmpeg.concat(vid, aud, v=1, a=1).node

        # Output trimmed video
        output = ffmpeg.output(joined[0], joined[1], returnVideoFolderName(videoId) + '/trimmed.mp4')
        output.run(overwrite_output=True)

        # Delete original video
        if os.path.exists(returnVideoDownloadLocation(videoId)):
            os.remove(returnVideoDownloadLocation(videoId))

        # Rename trimmed video to original name
        os.rename(returnVideoFolderName(videoId) + '/trimmed.mp4', returnVideoDownloadLocation(videoId))
        
    return


if __name__ == "__main__":
    import_video("YLslsZuEaNE",0,10)
