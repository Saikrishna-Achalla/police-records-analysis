"""
Utility functions used for the NextRequest scraper
"""

import re
import pandas as pd


def log_msg(msg, log=''):
    if log:
        with open(log, 'a') as f:
            f.write(msg)
    print(msg, end='')
        

def convert_requests_to_csv(requests, requests_name, path='data/', log=''):
    # Convert to DataFrame
    requests = [request for request in requests if (request and request['status'])]
    requests_df = pd.DataFrame(requests).drop_duplicates()

    # Create a zipped CSV file of the DataFrame
    try:
        compression_opts = dict(method='zip', archive_name=requests_name + '.csv')
        requests_df.to_csv(path + requests_name + '.zip', index=False, compression=compression_opts)
        log_msg('Successfully converted requests into CSV\n\n', log=log)
    except FileNotFoundError:
        log_msg('Unable to convert requests into CSV\n\n', log=log)


def scraper_progress(counter, start, end):
    """
    String displaying scraper progress
    """
    return 'Requests scraped: {:d}\tAvg runtime: {:.2f}s\tTotal runtime: {:.1f}s\n'.format(counter, (end - start) / counter, end - start)


def scraper_progress_final(counter, start, end, last_request):
    """
    String displaying final scraper progress
    """
    return 'Total requests scraped: {:d}\tAvg runtime: {:.2f}s\tTotal runtime: {:.1f}s\n\nLast request scraped: {}\n'.format(counter, (end - start) / counter, end - start, last_request)


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
