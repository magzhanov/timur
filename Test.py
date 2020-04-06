# in cmd:
# pip install pandas
# pip install grab
# pip install --upgrade lxml
# pip install pycurl
# MongoClient (https://docs.mongodb.com/manual/installation/)

import requests
import logging
import requests
import json
import pandas as pd
from pprint import pprint
from grab import Grab
from grab.spider import Spider, Task
from pymongo import MongoClient


grab = Grab()
url = "https://auctions.grantstreet.com/auctions/results"
grab.setup(post={"searching": 1, "from": 2004})
grab.go(url)

html = grab.doc.select("//table[@id='panel_2_table']//a[contains(@href,'html')]/@href")
print(len(html))
with open("2004.html", "w") as file:

    file.write(html)
