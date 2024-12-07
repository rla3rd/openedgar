"""
MIT License

Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Libraries
import logging
import urllib.parse
import time

# Packages
import dateutil.parser
import lxml.html
import requests
import edgar

# Project
from typing import Union

from config.settings.base import HTTP_SEC_HOST, HTTP_FAIL_SLEEP, HTTP_SEC_INDEX_PATH, HTTP_SLEEP_DEFAULT, EDGAR_IDENTITY

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)
edgar.set_identity(EDGAR_IDENTITY)


def get_buffer(remote_path: str, base_path: str = HTTP_SEC_HOST):
    """
    Retrieve a remote path to memory.
    :param remote_path: remote path on EDGAR to retrieve
    :param base_path: base path to prepend if not default EDGAR path
    :return: file_buffer, last_modified_date
    """
    # Log entrance
    logger.info("Retrieving remote path {0} to memory".format(remote_path))

    # Build URL
    remote_uri = urllib.parse.urljoin(base_path, remote_path.lstrip("/"))

    # Try to retrieve the file
    complete = False
    failures = 0
    file_buffer = None
    last_modified_date = None

    while not complete:
        try:
            with requests.Session() as s:
                r = s.get(remote_uri)
                if 'Last-Modified' in r.headers:
                    try:
                        last_modified_date = dateutil.parser.parse(r.headers['Last-Modified']).date()
                    except Exception as e:  # pylint: disable=broad-except
                        logger.error("Unable to update last modified date for {0}: {1}".format(remote_path, e))

                file_buffer = r.content
                complete = True

                # Sleep if set gt0
                if HTTP_SLEEP_DEFAULT > 0:
                    time.sleep(HTTP_SLEEP_DEFAULT)
        except Exception as e:  # pylint: disable=broad-except
            # Handle and sleep
            if failures < len(HTTP_FAIL_SLEEP):
                logger.warning("File {0}, failure {1}: {2}".format(remote_path, failures, e))
                time.sleep(HTTP_FAIL_SLEEP[failures])
                failures += 1
            else:
                logger.error("File {0}, failure {1}: {2}".format(remote_path, failures, e))
                return file_buffer, last_modified_date

    if b"SEC.gov | Request Rate Threshold Exceeded" in file_buffer:
        raise RuntimeError("Exceeded SEC request rate threshold; invalid data retrieved")
    elif b"SEC.gov | File Not Found Error Alert (404)" in file_buffer:
        raise RuntimeError("HTTP 404 for requested path")
    elif b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>" in file_buffer:
        raise RuntimeError("Access denied accessing path")

    # Log successful exit
    if complete:
        logger.info("Successfully retrieved file {0}; {1} bytes".format(remote_path, len(file_buffer)))

    return file_buffer, last_modified_date


def list_path(remote_path: str):
    """
    List a path on the EDGAR data store.
    :param remote_path: URL path to list
    :return:
    """
    # Log entrance
    logger.info("Retrieving directory listing from {0}".format(remote_path))
    remote_buffer, _ = get_buffer(remote_path)

    # Parse the index listing
    if remote_buffer is None:
        logger.warning("list_path for {0} was passed None buffer".format(remote_path))
        return []

    # Parse buffer to HTML
    html_doc = lxml.html.fromstring(remote_buffer)

    try:
        # Find links in directory listing
        link_list = html_doc.get_element_by_id("main-content").xpath(".//a")
        good_link_list = [l for l in link_list if "Parent Directory" not in
                          lxml.html.tostring(l, method="text", encoding="utf-8").decode("utf-8")]
        good_url_list = []

        # Populate new URL list
        for l in good_link_list:
            # Get raw HREF
            href = l.attrib["href"]
            if href.startswith("/"):
                good_url_list.append(href)
            else:
                good_url_list.append("/".join(s for s in [remote_path, href.lstrip("/")]))
    except KeyError as e:
        logger.error("Unable to find main-content tag in {0}; {1}".format(remote_path, e))
        return None

    # Log
    logger.info("Successfully retrieved {0} links from {1}".format(len(good_url_list), remote_path))
    return good_url_list


def list_index_by_year(year: int, pandas: bool=False):
    """
    Get list of index files for a given year.
    :param year: filing year to retrieve
    :return:
    """
    # Log entrance
    logger.info("Locating form index list for {0}".format(year))

    # Form index dataframe
    form_index = edgar.get_filings(year)
    form_ct = len(form_index)
    if pandas:
        form_index = form_index.to_pandas()
    
    # Log exit
    logger.info("Successfully located {0} form index files for {1}".format(form_ct, year))

    # Return
    return form_index


def list_index(min_year: int = 1950, max_year: int = 2050):
    """
    Get the list of form index files on SEC HTTP.
    :param min_year: min filing year to begin listing
    :param max_year: max filing year to list
    :return:
    """
    # Log entrance
    logger.info("Retrieving form index list")

    # Retrieve dataframe
    form_index_df = edgar.get_filings(range(min_year, max_year + 1)).to_pandas()

    # Log exit
    logger.info("Successfully located {0} form index files from {1} to {2}".format(form_index_df.shape[0], min_year, max_year))

    # Return
    return form_index_df

def get_cik_path(cik):
    """
    Get path on EDGAR or S3 for a given CIK.
    :param cik: company CIK
    :return:
    """
    return "edgar/data/{0}/".format(cik)
