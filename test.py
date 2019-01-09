import logging
import requests
from time import sleep

logging.basicConfig(level=logging.DEBUG)

headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
s = requests.Session()
s.headers.update(headers)

u = 'https://gamefaqs.gamespot.com/pc/197342-final-fantasy-viii/data'

for i in range(1):
    x = s.get(u).text

    sleep(0.34)
