import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import random
import time
from lxml.html import fromstring


randos = ["https://sandiego.nextrequest.com/documents","https://sandiego.nextrequest.com/requests/new","https://sandiego.nextrequest.com/users/sign_in"]
headers = requests.utils.default_headers()
headers.update({
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
})

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies


# PROXIES = list(get_proxies())

def get_data(url):
    
    def cleanhtml(raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    ids = url[-7:]
    
    # rand_proxy = random.randrange(len(PROXIES))
    # proxy = {'http':'http://'+PROXIES[rand_proxy],'https':'https://'+PROXIES[rand_proxy]}
    page = requests.get(url,headers = headers)#,proxies = proxy)
    soup = BeautifulSoup(page.content, 'html.parser')
    dept = soup.find_all(class_="current-department")
    depts = cleanhtml(str(dept[0])).strip()

    times = soup.find_all(class_="time-quotes")

    creation = cleanhtml(str(times[0])).strip()
    closing = cleanhtml(str(times[-1])).strip()
    
    
    def get_am_or_pm(time):
        if(time.partition("pm")[0][-2:] != 'am'):
#             print(time.partition("pm")[0][:-2],time.partition("pm")[0][:-2]!="am")
            return time.partition("pm")[0]+"pm"
        return time.partition("pm")[0]
    
    
    
    try:
        doj_creation = datetime.datetime.strptime(get_am_or_pm(creation), '%B %d, %Y, %I:%M%p')
        doj_closing  = datetime.datetime.strptime(get_am_or_pm(closing), '%B %d, %Y, %I:%M%p')
    except(ValueError):
        print(url)
        print(get_am_or_pm(creation))
        print(get_am_or_pm(closing))
    time_to_close = (doj_creation - doj_closing)

    return ids, depts, time_to_close


ids = []
depts = []
time_to_close = []

for i in range(21,22):
    for j in range(500,1000):
        time.sleep(15)
        url = "https://sandiego.nextrequest.com/requests/"+str(i) + "-" + str(j)
        # rand_proxy = random.randrange(len(PROXIES))
        # proxy = {'http':'http://'+ PROXIES[rand_proxy],'https':'https://'+PROXIES[rand_proxy]}
        if(j%19 ==0):
            rand_index = random.randrange(len(randos))
            requests.get(randos[rand_index])
        elif(requests.get(url,headers = headers).url != 'https://sandiego.nextrequest.com/requests'):
            print(url)
            req_id, dept, t = get_data(url)
            ids.append(req_id)
            depts.append(dept)
            time_to_close.append(t)

"""
TODO:
ids = []
depts = []
time_to_close = []
for url in urls:
    id, dept, t = get_data(url)
    ids.append(id)
    depts.append(dept)
    time_to_close.append(t)
result_df = pd.DataFrame(data = {'Request ID': ids,
                    'Department': depts,
                    'Time to Close': time_to_close}) 


"""

result_df = pd.DataFrame(data = {'Request ID': ids,
                    'Department': depts,
                    'Time to Close': time_to_close})
