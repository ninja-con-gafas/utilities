"""
The module provides utilities to interact with YouTube using YouTube Data API.
"""


import json

from googleapiclient import discovery, errors
from socket import timeout
from typing import Dict


def get_credentials(path: str) -> Dict[str, str]:
    """
    Load API credentials from a JSON file.

    args:
        path (str): Path to the credentials JSON file.

    returns:
        dict: Dictionary containing API credentials.
    """
    with open(path) as credentials_path:
        return json.load(credentials_path)
        
def get_video_url(developer_key: str, service_name: str, query: str, version: str) -> str:
    """
    Get the YouTube video URL corresponding to the query.

    args:
        developer_key (str): YouTube Data API developer key.
        service_name (str): Name of the YouTube API service.
        query (str): The search query string (title and artist of the song).
        version (str): Version of the YouTube API.

    returns:
        str: URL of the YouTube video or an empty string if no video is found.

    raises:
        errors.HttpError: If the YouTube API request fails.
        timeout: If the request times out.
    """
    print(f"Getting video ID for {query}")
    try:
        video_id: str = discovery.build(developerKey=developer_key,
                                        num_retries=5,
                                        serviceName=service_name,
                                        version=version).search() \
            .list(maxResults=1,
                  part="id",
                  q=f"{query}",
                  type="video",
                  videoDefinition="high",
                  videoDuration="any") \
            .execute().get("items", [{}])[0].get("id", {}).get("videoId")

        if video_id:
            print(f"{video_id} is the video ID of {query}")
            return f"https://youtu.be/{video_id}"
        else:
            print(f"No video found for {query}")
            return ""
    except (errors.HttpError, timeout) as exception:
        print(f"Error fetching video for {query}: {exception}")
        return ""
