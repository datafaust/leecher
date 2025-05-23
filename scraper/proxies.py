import random
from scraper.config import USE_PROXIES

PROXY_LIST = [
    "38.153.152.244:9594:rolorsci:d8clzwimh7dp",
    "185.199.231.45:8382:rolorsci:d8clzwimh7dp"
]

def get_random_proxy():
    proxy = random.choice(PROXY_LIST)
    host, port, user, pwd = proxy.split(":")
    return {
        "http": f"http://{user}:{pwd}@{host}:{port}",
        "https": f"http://{user}:{pwd}@{host}:{port}"
    }

def maybe_use_proxy(prob=0.8):
    if not USE_PROXIES:
        return None
    return get_random_proxy() if random.random() < prob else None