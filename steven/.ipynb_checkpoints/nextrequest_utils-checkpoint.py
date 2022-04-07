"""
Utility functions used for the NextRequest scraper
"""

import re
import pandas as pd


def convert_requests_to_csv(requests, requests_name):
    # Convert to DataFrame
    requests = [request for request in requests if (request and request['status'])]
    requests_df = pd.DataFrame(requests).drop_duplicates()

    # Create a zipped CSV file of the DataFrame
    compression_opts = dict(method='zip', archive_name=requests_name + '.csv')
    requests_df.to_csv('data/' + requests_name + '.zip', index=False, compression=compression_opts)


def print_progress(counter, start, end):
    """
    Prints scraper progress
    """
    print('Requests scraped:', counter,
          '\tAvg runtime:', str(round((end - start) / counter, 2)) + 's/request',
          '\tTotal runtime:', str(round(end - start, 1)) + 's')


def print_progress_final(counter, start, end, last_request):
    """
    Prints final scraper progress
    """
    print('Total requests scraped:', counter,
          '\tAvg runtime:', str(round((end - start) / counter, 2)) + 's/request',
          '\tTotal runtime:', str(round(end - start, 1)) + 's')
    print()
    print('Last request scraped:', last_request)
    print()


def get_city_from_url(url):
    """
    Finds the city name from the NextRequest URL.
    """
    return re.match(r'(?<=https://)[a-zA-Z]*', url)[0]


def get_webelement_text(webelement):
    """
    Gets the text of each web element in a list, if such a list exists.
    """
    return list(map(lambda x: x.text, webelement)) if webelement else []


def get_webelement_link(webelement):
    """
    Gets the link of each web element in a list, if such a list exists.
    """
    return list(map(lambda x: x.get_attribute('href'), webelement)) if webelement else []


def remove_download_from_urls(urls):
    """
    Removes '/download' from the end of a list of URLs, if the list exists.
    """
    return list(map(lambda url: re.match(r'.*(?=/download)', url)[0], urls)) if urls else []