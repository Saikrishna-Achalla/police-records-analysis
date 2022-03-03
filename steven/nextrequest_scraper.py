from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import traceback
from timeit import default_timer as timer
from time import sleep

from nextrequest_utils import *


class NextRequestScraper:
    """
    Scraper scripts for NextRequest request databases. Currently does not scrape all possible documents from each request due to difficulties with navigation bars, but the number of such documents can be recovered from the messages.
    """
    def __init__(self, driver, url, wait_time=1):
        self.driver = driver if 'webdriver' in str(type(driver)) else webdriver.Firefox()
        self.driver.implicitly_wait(wait_time)
        self.url = url if ((type(url) == str) and ('nextrequest.com' in url) and ('requests/' in url)) \
            else 'https://lacity.nextrequest.com/requests/'

    def scrape(self, requests, earliest_id, requests_name='requests',
               num_requests=-1, timeout=10, progress=100, debug=0):
        """
        Main scraper routine
        TODO: Add better documentation
        """
        num_its = 1  # Keeps track of how many times the scraper has been run

        # Initialize the current ID to be either the earliest ID possible if the requests list is empty, or the last ID
        # in the list
        current_id = requests[-1]['id'] if requests else earliest_id

        # Start by running an initial iteration of the scraper
        self.driver.get(self.url + current_id)

        # Print iteration number. TODO: Replace with file write
        it_num_title = 'Iteration ' + str(num_its)
        print(it_num_title)
        print('-' * len(it_num_title))

        # Re-scrape the current request
        start_id = requests.pop()['id'] if requests else current_id
        print('Starting request:', start_id)
        print()

        # Scrape requests until the scraper either reaches the end of the database or times out
        try:
            self.scrape_requests_sequential(requests,
                                            num_requests=num_requests,
                                            progress=progress,
                                            debug=debug)
        except KeyboardInterrupt:
            convert_requests_to_csv(requests, requests_name)
            return len(requests)

        num_its += 1
        sleep(timeout)  # Wait after the script reaches the end of the database or after a timeout

        # Restart the driver at the last request scraped
        current_id = requests[-1]['id']
        self.driver.get(self.url + current_id)

        # Continue to scrape until the scraper reaches the end of the database or times out
        try:
            while self.driver.find_elements(By.CLASS_NAME, 'js-next-request'):
                it_num_title = 'Iteration ' + str(num_its)
                print(it_num_title)
                print('-' * len(it_num_title))

                print('Starting request:', requests.pop()['id'])
                print()

                self.scrape_requests_sequential(requests,
                                                num_requests=num_requests,
                                                progress=progress,
                                                debug=debug)

                num_its += 1
                sleep(timeout)

                current_id = requests[-1]['id']
                self.driver.get(self.url + current_id)
        except KeyboardInterrupt:
            pass

        convert_requests_to_csv(requests, requests_name)
        return len(requests)

    def scrape_requests_sequential(self, requests, num_requests=-1, progress=0, debug=0):
        """
        Scrapes all records on a NextRequest request database starting from the given ID and
        moving forward chronologically until the number of requests scraped reaches a given
        number. Each scraped requests is added to a given list. If num_requests is non-positive,
        then scrape as many records as possible.
        """
        start = timer()  # Timer for progress checking purposes
        counter = 0  # Keeps track of how many requests have been scraped

        # Start by scraping the initial record. TO-DO: Add try-except-finally blocks for KeyboardInterrupt errors

        # Only scrape a request if it was loaded properly; otherwise, stop the scraper
        if not self.driver.find_elements(By.CLASS_NAME, 'nextrequest'):
            print('No requests scraped')
            return counter

        # Scrape initial request
        try:
            self.scrape_request(requests, counter=counter, debug=debug)
        except KeyboardInterrupt:
            print('No requests scraped')
            return counter

        counter += 1

        # For positive num_requests, return the list of requests if the counter reaches the desired number
        if (num_requests > 0) and (counter == num_requests):
            if progress:
                print_progress_final(counter, start, end=timer(), last_request=requests[-1]['id'])

            return counter

        # Show progress, if desired
        if progress and (counter % progress == 0):
            print_progress(counter, start, end=timer())

        # Continue to scrape until it is not possible to navigate to the next request,
        # either due to the scraper reaching the end of the database or because of a
        # timeout
        try:
            while self.driver.find_elements(By.CLASS_NAME, 'js-next-request'):
                self.driver.find_element(By.CLASS_NAME, 'js-next-request')\
                    .click()  # Click on the arrow to navigate to the next request

                if not self.driver.find_elements(By.CLASS_NAME, 'nextrequest'):
                    break

                self.scrape_request(requests, counter=counter, debug=debug)

                counter += 1

                if (num_requests > 0) and (counter == num_requests):
                    break

                if progress and (counter % progress == 0):
                    print_progress(counter, start, end=timer())
        except KeyboardInterrupt:
            pass
        finally:
            # Final progress check
            if progress:
                print_progress_final(counter, start, end=timer(), last_request=requests[-1]['id'])

            return counter

    def scrape_request(self, requests, counter=-1, debug=0):
        """
        Scrapes data about a given request on a NextRequest request database, appending the result
        to the given list.
        """
        request_id, status, desc, date, depts, poc, events, docs = [None] * 8  # Initialize variables
        try:  # Attempt to scrape relevant data
            request_id = self.driver.find_element(By.CLASS_NAME, 'request-title-text').text.split()[1][1:]  # Request ID
            status = self.driver.find_element(By.CLASS_NAME, 'request-status-label').text.strip()  # Request status

            desc_row = self.driver.find_element(By.CLASS_NAME, 'request-text')  # Box containing request description
            for desc_read_more in desc_row.find_elements(By.PARTIAL_LINK_TEXT, 'Read more'):  # Expand description if necessary
                desc_read_more.click()
            desc = desc_row.find_element(By.ID, 'request-text').text  # Full request description
            
            date = self.driver.find_element(By.CLASS_NAME, 'request_date').text  # Request date
            depts = self.driver.find_element(By.CLASS_NAME, 'current-department').text  # Department(s) assigned to the request
            poc = self.driver.find_element(By.CLASS_NAME, 'request-detail').text  # Point of contact

            # Documents attached to the request, if there are any (CURRENTLY DOES NOT SCRAPE ALL DOCUMENTS)
            doc_list = self.driver.find_element(By.CLASS_NAME, 'document-list')  # Box containing documents
            if '(none)' not in doc_list.text:  # Check for the presence of documents
                # Expand folders, if there are any
                folders = doc_list.find_elements(By.CLASS_NAME, 'folder-toggle')
                for folder in folders:
                    folder.click()
                
                docs_all = doc_list.find_elements(By.CLASS_NAME, 'document-link')

                # TODO: Figure out how to scrape all documents from a request whose folders also have navigation bars
                #             # If there are many documents, then there will be navigation bar(s)
                #             pag_navs = doc_list.find_elements_by_class_name('pagy-nav')
                #             if pag_navs:
                #                 pag_nav = pag_navs[-1]
                #                 while not pag_nav.find_elements_by_class_name('page.next.disable'):
                #                     pag_nav.find_element_by_partial_link_text('Next').click()
                #                     doc_list = driver.find_element_by_class_name('document-list')
                #                     doc_titles.extend(get_webelement_text(doc_list.find_elements_by_class_name('document-link')))
                #                     doc_links.extend(remove_download_from_urls(get_webelement_link(doc_list.find_elements_by_class_name('document-link'))))=
                #             doc_titles = list(set(doc_titles))
                #             doc_links = list(set(doc_links))

                # DataFrame-converted-to-CSV consisting of all documents
                docs = pd.DataFrame({
                    'title': get_webelement_text(docs_all),
                    'link': remove_download_from_urls(get_webelement_link(docs_all))
                }).to_csv(index=False)

            # Messages recorded on the request page, if there are any
            event_history = self.driver.find_elements(By.CLASS_NAME, 'generic-event')  # All message blocks
            if event_history:  # Check for presence of messages
                num_events = len(event_history)

                # Titles, descriptions, and time strings for each message
                event_titles = [None] * num_events
                event_items = [None] * num_events
                time_quotes = [None] * num_events

                # Scrape information from each individual event
                for i in range(len(event_history)):
                    event = event_history[i]

                    event_title = event.find_element(By.CLASS_NAME, 'event-title').text  # Event title
                    for details_toggle in event.find_elements(By.PARTIAL_LINK_TEXT, 'Details'):  # Expand event item details
                        details_toggle.click()
                    event_item = '\n'.join(get_webelement_text(event.find_elements(By.CLASS_NAME, 'event-item')))  # Event item
                    time_quote = event.find_element(By.CLASS_NAME, 'time-quotes').text  # Time quote

                    event_titles[i] = event_title
                    event_items[i] = event_item
                    time_quotes[i] = time_quote

                # DataFrame-converted-to-CSV consisting of all messages
                events = pd.DataFrame({
                    'title': event_titles,
                    'item': event_items,
                    'time': time_quotes
                }).to_csv(index=False)

            # For testing purposes, print a message whenever a request is successfully scraped
            if debug:
                print(request_id, 'scraped')
        except:  # If an exception occurs, print the stack trace
            print('Exception occurred' + (' at count ' + str(counter + 1) if counter >= 0 else '') + ':')
            traceback.print_exc()
            print()
        finally:  # Append the request to the list
            requests.append({
                'id': request_id,
                'status': status,
                'desc': desc,
                'date': date,
                'depts': depts,
                'docs': docs,
                'poc': poc,
                'msgs': events
            })
