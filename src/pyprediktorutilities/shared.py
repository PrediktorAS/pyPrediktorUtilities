import requests
from pydantic import AnyUrl, validate_call
from typing import Literal
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@validate_call
def validate_file(file: str):
    if not Path(file).is_file():
        logging.error(f"File {file} does not exist")
        raise FileNotFoundError(f"File {file} does not exist")


@validate_call
def request_from_api(
    rest_url: AnyUrl,
    method: Literal["GET", "POST"],
    endpoint: str,
    data: str = None,
    params: dict = None,
    headers: dict = None,
    extended_timeout: bool = False,
) -> str:
    """Function to perform request against a REST API

    Args:
        rest_url (str): The URL with trailing shash
        method (str): "GET" or "POST"
        endpoint (str): The last part of the url (without the leading slash)
        data (str): defaults to None but can contain the data to send to the endpoint
        headers (str): default to None but can contain the headers og the request
    Returns:
        JSON: The result if successfull
    """
    request_timeout = (3, 300 if extended_timeout else 27)
    combined_url = f"{rest_url}{endpoint}"
    if method == "GET":
        result = requests.get(combined_url, timeout=request_timeout, params=params, headers=headers)

    if method == "POST":
        result = requests.post(
            combined_url, data=data, headers=headers, timeout=request_timeout, params=params
        )

    result.raise_for_status()
    return result.json()
