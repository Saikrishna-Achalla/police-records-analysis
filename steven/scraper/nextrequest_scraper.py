from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException

import traceback
from timeit import default_timer as timer
from time import sleep
from datetime import datetime

from nextrequest_scraper_utils import *


class NextRequestScraper:
    """
    Scraper scripts for NextRequest request databases. Currently does not scrape all possible documents from each
    request due to difficulties with navigation bars, but the number of such documents can be recovered from the
    messages.
    """

    def __init__(self, driver, url, wait_time=0.1):
        self.driver = driver if 'webdriver' in str(type(driver)) else webdriver.Firefox()
        self.driver.implicitly_wait(wait_time)
        self.url = url if ((type(url) == str) and ('nextrequest.com' in url) and ('requests/' in url)) \
            else 'https://lacity.nextrequest.com/requests/'

    def scrape(self, requests, earliest_id, requests_name='requests', path='data/',
               num_requests=-1, timeout=10, progress=100, debug=0, log=''):
        """
        Main scraper routine
        TODO: Add better documentation
        """
        num_its = 1  # Keeps track of how many times the scraper has been (re-)run
        it_num_line = ''  # For warning suppression purposes
        if log == -1: log = path + requests_name + '.log'  # If no directory is specified, point to this one

        # Initialize the current ID to be either the earliest ID possible if the requests list is empty, or the last ID
        # in the list
        if requests:
            current_id = requests.pop()['id']
            num_requests += 1
        else:
            current_id = earliest_id

        # Get the initial request URL
        self.driver.get(self.url + current_id)

        # Initial log message
        log_msg('Start time: {}\n\n'.format(str(datetime.now())), log=log)

        while True:
            try:
                it_num_title = 'Iteration ' + str(num_its)
                it_num_line = '-' * len(it_num_title)
                log_msg('{}\n{}\n'.format(it_num_title, it_num_line), log=log)

                # Scrape as many requests as possible, subtracting from the total number of requests when we do so
                num_requests -= self.scrape_requests_sequential(requests, current_id,
                                                                num_requests=num_requests,
                                                                progress=progress,
                                                                debug=debug, log=log)
                num_its += 1  # Increase number of iterations
                log_msg('{}\n\n'.format(it_num_line), log=log)

                # Stop scraping if number of requests reached
                if not num_requests: break

                sleep(timeout)  # Wait for the specified amount of time before restarting the driver

                current_id = requests[-1]['id']  # Restart the driver at the last request scraped
                self.driver.get(self.url + current_id)  # Get the request URL
                self.driver.find_element(By.CLASS_NAME,
                                         'js-next-request')  # Check if there are more requests after the current one

                requests.pop()
                num_requests += 1  # Since the last request will be re-scraped, increase # of requests left to scrape by 1
            except InterruptScrapeException:
                log_msg('{}\n\n'.format(it_num_line), log=log)
                break
            except NoSuchElementException:
                log_msg('Webdriver could not find js-next-request element between scraper iterations\n{}\n\n'.format(
                    it_num_line), log=log)
                break
            except KeyboardInterrupt:
                log_msg('User interruption occurred between scraper iterations\n{}\n\n'.format(it_num_line), log=log)
                break
            except:
                log_msg('Exception occurred between scraper iterations\n{}\n{}\n\n'.format(traceback.format_exc(),
                                                                                           it_num_line), log=log)
                break

        convert_requests_to_csv(requests, requests_name, path=path, log=log)
        log_msg('End time: {}\n\n***\n\n'.format(str(datetime.now())), log=log)
        return len(requests)

    def scrape_requests_sequential(self, requests, start_id, num_requests=-1, progress=0, debug=0, log=''):
        """
        Scrapes all records on a NextRequest request database starting from the ID URL passed into the driver and
        moving forward chronologically until the number of requests scraped reaches a given
        number. Each scraped requests is added to the given list. If num_requests is non-positive,
        then scrape as many records as possible.
        """
        start = timer()  # Timer for progress checking purposes
        counter = 0  # Keeps track of how many requests have been scraped

        # Show the starting ID, if desired
        if progress: log_msg('Starting request: {}\n\n'.format(start_id), log=log)

        # If no requests are wanted, then return immediately
        if num_requests == 0:
            if progress: log_msg('No requests scraped\n\n', log=log)
            return counter

        # Scrape until it is not possible to navigate to the next request, either due to the scraper reaching the end of the database or because of a timeout
        while True:
            try:
                counter += self.scrape_request(requests, counter=counter, debug=debug, log=log)

                # Exit the loop if the number of requests is reached
                if counter == num_requests:
                    break

                # Show scraper progress if desired
                if progress and (counter % progress == 0):
                    log_msg(scraper_progress(counter, start, end=timer()), log=log)

                self.driver.find_element(By.CLASS_NAME,
                                         'js-next-request').click()  # If possible, navigate to the next request
            except NoSuchElementException:  # Exit the loop if the js-next-request element cannot be found
                log_msg('Webdriver could not find js-next-request element after count {}\n'.format(counter), log=log)
                break
            except InterruptScrapeException:  # Handling for any exception thrown while a request was being scraped
                if progress:
                    log_msg(scraper_progress_final(counter + 1, start, end=timer(), last_request=requests[-1]['id']),
                            log=log)
                raise InterruptScrapeException
            except KeyboardInterrupt:  # Handling for any exception thrown in between scraping requests
                log_msg('User interruption occurred after count {}\n'.format(counter), log=log)
                if progress:
                    log_msg(scraper_progress_final(counter + 1, start, end=timer(), last_request=requests[-1]['id']),
                            log=log)
                raise InterruptScrapeException
            except:
                log_msg('Exception occurred after count {}\n{}\n'.format(counter, traceback.format_exc()), log=log)
                break

        # Show scraper progress if desired
        if progress:
            log_msg(scraper_progress_final(counter, start, end=timer(), last_request=requests[-1]['id']), log=log)

        # Return the number of requests scraped
        return counter

    def scrape_request(self, requests, counter=-1, debug=0, log=''):
        """
        Scrapes data about a given request on a NextRequest request database, appending the result
        to the given list.
        """
        request_id, status, desc, date, depts, req, fee, poc, events, docs = [None] * 10  # Initialize variables
        count_err_msg = (' while scraping count {}'.format(counter + 1) if counter >= 0 else '') + '\n'

        try:  # Attempt to scrape relevant data
            request_id = self.driver.find_element(By.CLASS_NAME, 'request-title-text').text.split()[1][1:]  # Request ID
            status = self.driver.find_element(By.CLASS_NAME, 'request-status-label').text.strip()  # Request status

            desc_row = self.driver.find_element(By.CLASS_NAME, 'request-text')  # Box containing request description
            for desc_read_more in desc_row.find_elements(By.PARTIAL_LINK_TEXT,
                                                         'Read more'):  # Expand description if necessary
                desc_read_more.click()
            desc = desc_row.find_element(By.ID, 'request-text').text  # Full request description

            date = self.driver.find_element(By.CLASS_NAME, 'request_date').text  # Request date
            depts = self.driver.find_element(By.CLASS_NAME,
                                             'current-department').text  # Department(s) assigned to the request
            poc = self.driver.find_element(By.CLASS_NAME, 'request-detail').text  # Point of contact

            # Documents attached to the request, if there are any. Currently only scrapes the first 50 or so documents
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
                    for details_toggle in event.find_elements(By.PARTIAL_LINK_TEXT,
                                                              'Details'):  # Expand event item details
                        details_toggle.click()
                    event_item = '\n'.join(
                        get_webelement_text(event.find_elements(By.CLASS_NAME, 'event-item')))  # Event item
                    time_quote = event.find_element(By.CLASS_NAME, 'time-quotes').text  # Time quote

                    event_titles[i] = event_title
                    event_items[i] = event_item
                    time_quotes[i] = time_quote

                # DataFrame, converted to CSV, consisting of all messages
                events = pd.DataFrame({
                    'title': event_titles,
                    'item': event_items,
                    'time': time_quotes
                }).to_csv(index=False)

            # For testing purposes, print a message whenever a request is successfully scraped
            if debug:
                log_msg('{} scraped'.format(request_id), log=log)
        except NoSuchElementException:
            log_msg('Webdriver could not find element{}'.format(count_err_msg), log=log)
        except StaleElementReferenceException:
            log_msg('Stale element referenced{}'.format(count_err_msg), log=log)
        except TimeoutException:
            log_msg('Webdriver timed out{}'.format(count_err_msg), log=log)
        except KeyboardInterrupt:  # Raise a special exception if an interruption occurs while a request is being scraped
            log_msg('User interruption occurred{}'.format(count_err_msg), log=log)
            raise InterruptScrapeException
        except:
            log_msg('Exception occurred{}\n{}'.format(count_err_msg, traceback.format_exc()), log=log)
        finally:  # Always append the scraped request data to the list regardless of completeness
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

        return 1


class InterruptScrapeException(Exception):
    """
    A special exception class used to indicate that a KeyboardInterrupt exception was thrown while the scraper was running
    """
    pass
