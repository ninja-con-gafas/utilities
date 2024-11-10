"""
YouTube audio downloader downloads the best available audio stream for the provided list of YouTube video URLs in mp3
format.

The script:

    1. Reads the `url` column from the CSV file.
    2. Retrieves YouTube video title and author name for the video.
    3. Downloads the best audio stream for the YouTube video as mp3 file.
    4. Saves the report of the download process as a CSV file.

dependencies:
    - yt-dlp requires ffmpeg to be installed.

usage:
    python3 youtube_audio_downloader.py **/url.csv
"""

from concurrent import futures
from os import listdir, path
from pandas import DataFrame, read_csv
from re import sub
from sys import argv
from youtube import download_audio_as_mp3, get_video_id, get_video_metadata

DOWNLOADS_PATH = path.expanduser("~/Downloads/")

def is_audio_downloaded(video_id: str) -> bool:

    """
    Check if the audio stream of a video with the given video_id exists in the DOWNLOADS_PATH. The audio file has the
    video_id as the last word in the filename, followed by '.mp3'.

    args:
        video_id (str): The video ID to search for.

    returns:
        bool: True if the video is found, False otherwise.

    raises:
        None
    """

    for filename in listdir(DOWNLOADS_PATH):
        if filename.endswith('.mp3'):
            file_video_id = filename[:-4].split()[-1]
            if file_video_id == video_id:
                return True
    return False

def main() -> None:

    """
    Main function to download the audio tracks of all the YouTube videos in the URL CSV file.
    """

    if len(argv) != 2:
        print(f"Usage: python3 {argv[0]} **/url.csv")
    else:
        url: DataFrame = (read_csv(filepath_or_buffer=argv[1])
        .assign(video_id=lambda x: x['url'].apply(get_video_id))
        .drop_duplicates(subset=['video_id'])
        .assign(
            metadata=lambda x: x['video_id'].apply(get_video_metadata),
            name=lambda x: x.apply(
                lambda row: sub(
                    r'[^a-zA-Z0-9]',
                    ' ',
                    f"{row['metadata'].get('title')} {row['metadata'].get('author_name')}")
                            + f" {row['video_id']}",
                axis=1)))

        url.to_csv(path_or_buf=f"{DOWNLOADS_PATH}/youtube_audio_downloader_report.csv", index=False)

        with futures.ThreadPoolExecutor() as executor:
            executor.map(lambda row: download_audio_as_mp3(download_path=DOWNLOADS_PATH,
                                                           file_name=(row["name"]),
                                                           url=row["url"]),
                         url.to_dict(orient="records"))

        (url.assign(is_downloaded=lambda x: x["video_id"].apply(is_audio_downloaded))
         .to_csv(path_or_buf=f"{DOWNLOADS_PATH}/youtube_audio_downloader_report.csv", index=False))

if __name__ == "__main__":
    main()