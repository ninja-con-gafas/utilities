"""
The module provides utilities to interact with Google AI.
"""

from requests import post, Response

def get_api_key(path: str) -> str:

    """
    Read API key from a text file.

    args:
        path (str): Path to the API key text file.

    returns:
        str: API key
    """

    print(f"Reading Gemini Developer API key from the file: {path}")
    with open(path) as api_key:
        return api_key.read()

def get_response(api_key: str, prompt:str) -> Response:

    """
    Sends a POST request to the Google Gemini 1.5 Flash Large Language Model to generate content based on the provided
    prompt.

    args:
        api_key (str): The API key used for authentication with the Google Gemini Language Model API.
        prompt (str): The text prompt to send to the API for content generation.

    returns:
        Response: The response object from the API call containing the generated content and response details.
    """

    print(f"Getting response from Gemini 1.5 Flash for the given prompt: {prompt}")
    headers = {
        'Content-Type': 'application/json',
    }

    params = {
        'key': api_key,
    }

    json_data = {
        'contents': [
            {
                'parts': [
                    {
                        'text': prompt,
                    },
                ],
            },
        ],
    }

    return post(
        url='https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent',
        params=params,
        headers=headers,
        json=json_data,
    )