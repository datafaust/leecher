import requests
import re
from bs4 import BeautifulSoup
import urllib.parse
from scraper.headers import get_random_headers
from scraper.proxies import maybe_use_proxy

def get_real_website_from_yelp(yelp_url):
    try:
        res = requests.get(yelp_url, headers=get_random_headers(), proxies=maybe_use_proxy(), timeout=10)
        if res.status_code != 200:
            return 'N/A'

        soup = BeautifulSoup(res.text, 'html.parser')
        redirect_links = soup.find_all('a', href=re.compile(r'/biz_redir\?url='))

        for link in redirect_links:
            href = link.get('href')
            parsed = urllib.parse.urlparse(href)
            query = urllib.parse.parse_qs(parsed.query)
            if 'url' in query:
                return urllib.parse.unquote(query['url'][0])
        return 'N/A'
    except Exception as e:
        print(f"Error fetching real website: {e}")
        return 'N/A'
