"""
Shazam automates the process of extracting unique Shazam tracks from the Shazam data file, retrieving YouTube URLs for
the tracks and downloading the audio in mp3 format.

The script:

    1. Extracts Shazam data from the Shazam CSV data file, removes duplicates and irrelevant columns.
    2. Retrieves YouTube URLs for the songs using the YouTube API.
    3. Saves the final list of Shazam tracks with YouTube URLs to a CSV file.
    4. Downloads the best audio stream for the Shazam track from YouTube as mp3 file.

dependencies:
    - yt-dlp requires ffmpeg to be installed.

usage:
    python3 shazam.py ../SyncedShazams.csv ../credentials/shazam.json
"""


import yt_dlp

from google.youtube import get_credentials, get_video_url
from concurrent import futures
from os import path
from pandas import DataFrame, read_csv
from sys import argv
from typing import Callable, Any

DOWNLOADS_PATH = path.expanduser("~/Downloads/")

def download_audio_as_mp3(file_name: str, url: str) -> None:

    """
    Download the best available audio stream from a YouTube video and save it as a mp3 file.

    args:
        file_name (str): Name of the mp3 file to save the audio stream.
        url (str): URL of the YouTube video to download.

    returns:
        None

    raises:
        yt_dlp.utils.DownloadError: If there is an error during the download process.
        yt_dlp.utils.ExtractorError: If there is an error extracting the video information.
    """

    try:
        print(f"Downloading audio stream for {file_name} from {url}")
        ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "0",
            }],
            "outtmpl": f"{DOWNLOADS_PATH}{file_name}.%(ext)s",
            'socket_timeout': 30,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except (AttributeError, TypeError, ValueError,
            yt_dlp.utils.DownloadError, yt_dlp.utils.ExtractorError) as exception:
        print(f"Error downloading {file_name}: {exception} for URL {url}")


def extract_shazams(path: str) -> DataFrame:

    """
    Extract unique Shazam tracks from a CSV file, dropping unnecessary columns.

    args:
        path (str): Path to the Shazam CSV file.

    returns:
        DataFrame: A DataFrame containing the unique Shazam tracks.
    """

    return read_csv(filepath_or_buffer=path) \
        .drop_duplicates(subset=["artist", "title"]) \
        .drop(columns=["date", "latitude", "longitude", "status"], errors="ignore") \
        .sort_values(by=["artist", "title"])


def process(function: Callable[..., Any], **kwargs: Any):

    """
    Execute a function with the provided keyword arguments.

    args:
        function (Callable): The function to be executed.
        **kwargs (Any): The arguments to pass to the function.

    returns:
        Any: The result of the function execution.
    """

    return function(**kwargs)


def main() -> None:

    """
    Main function to orchestrate extraction of Shazam data, retrieval of YouTube URLs and download the audio tracks.
    """

    if len(argv) != 2:
        print(f"Usage: python3 {argv[0]} ../SyncedShazams.csv ../credentials/shazam.json")
    else:
        shazams: DataFrame = extract_shazams(path=argv[1])
        credentials = get_credentials(path=argv[2])

        shazams["url"] = (shazams["title"] + " " + shazams["artist"])\
                          .apply(func=lambda query: get_video_url(developer_key=credentials.get("api_key"),
                                                                  service_name=credentials.get("service_name"),
                                                                  version=credentials.get("version"),
                                                                  query=query))

        shazams.to_csv(path_or_buf=f"{DOWNLOADS_PATH}shazams.csv", index=False)

        with futures.ThreadPoolExecutor() as executor:
            executor.map(lambda row: process(download_audio_as_mp3, file_name=row["title"], url=row["url"]),
                         shazams.to_dict(orient="records"))


if __name__ == "__main__":
    main()
