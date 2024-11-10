"""
Shazam automates the extraction process of unique music tracks from the Shazam data file and downloads the best
available audio stream of the music track from YouTube in mp3 format.

The script:

    1. Extracts records from the Shazam CSV data file, removes duplicates and irrelevant columns.
    2. Retrieves YouTube URLs for the music.
    3. Downloads the best audio stream for the track from YouTube as mp3 file.
    4. Saves the report of the download process as a CSV file.

dependencies:
    - yt-dlp requires ffmpeg to be installed.

usage:
    python3 shazams_downloader.py **/SyncedShazams.csv
"""

from concurrent import futures
from os import listdir, path
from pandas import DataFrame, read_csv
from sys import argv
from youtube import download_audio_as_mp3, get_video_id, search_youtube

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


def is_audio_downloaded(video_id):

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
    Main function to orchestrate the extraction of Shazam data, retrieval of YouTube URLs and download the audio tracks.
    """

    if len(argv) != 2:
        print(f"Usage: python3 {argv[0]} **/SyncedShazams.csv")
    else:
        shazams: DataFrame = (extract_shazams(file_path=argv[1])
                              .assign(
            url=lambda x: x.apply(lambda row: search_youtube(f"{row['title']} {row['artist']} lyrics")[0], axis=1)
            , video_id=lambda x: x['url'].apply(get_video_id)))

        shazams.to_csv(path_or_buf=f"{DOWNLOADS_PATH}/shazams_downloader_report.csv", index=False)

        with futures.ThreadPoolExecutor() as executor:
            executor.map(lambda row: download_audio_as_mp3(download_path=DOWNLOADS_PATH,
                                                           file_name=f"{row["title"]} {row["artist"]} {row["video_id"]}",
                                                           url=row["url"]),
                         shazams.to_dict(orient="records"))

        (shazams.assign(is_downloaded=lambda x: x["video_id"].apply(is_audio_downloaded))
         .to_csv(path_or_buf=f"{DOWNLOADS_PATH}/shazams_downloader_report.csv", index=False))

if __name__ == "__main__":
    main()