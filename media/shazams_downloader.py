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

from concurrent import futures
from iam import read_api_key
from os import path
from pandas import DataFrame, read_csv
from sys import argv
from youtube import download_audio_as_mp3, get_video_url

DOWNLOADS_PATH = path.expanduser("~/Downloads/")

def extract_shazams(file_path: str) -> DataFrame:

    """
    Extract unique Shazam tracks from a CSV file, dropping unnecessary columns.

    args:
        file_path (str): Path to the Shazam CSV file.

    returns:
        DataFrame: A DataFrame containing the unique Shazam tracks.
    """

    return read_csv(filepath_or_buffer=file_path) \
        .drop_duplicates(subset=["artist", "title"]) \
        .drop(columns=["date", "latitude", "longitude", "status"], errors="ignore") \
        .sort_values(by=["artist", "title"])

def main() -> None:

    """
    Main function to orchestrate extraction of Shazam data, retrieval of YouTube URLs and download the audio tracks.
    """

    if len(argv) != 2:
        print(f"Usage: python3 {argv[0]} **/SyncedShazams.csv **/credentials/shazam.json")
    else:
        shazams: DataFrame = extract_shazams(file_path=argv[1])
        api_key = read_api_key(path=argv[2])

        shazams["url"] = ((shazams["title"] + " " + shazams["artist"])
                          .apply(func=lambda query: get_video_url(api_key=api_key, query=query)))

        shazams.to_csv(path_or_buf=f"{DOWNLOADS_PATH}shazams.csv", index=False)

        with futures.ThreadPoolExecutor() as executor:
            executor.map(lambda row: download_audio_as_mp3(download_path=DOWNLOADS_PATH,
                                                           file_name=row["title"],
                                                           url=row["url"]),
                         shazams.to_dict(orient="records"))

if __name__ == "__main__":
    main()