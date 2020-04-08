# in cmd:
# pip install pandas
# pip install grab
# pip install --upgrade lxml
# pip install pycurl
# MongoClient (https://docs.mongodb.com/manual/installation/)

import requests
import logging
import requests

# Import our classes
from Auctions import FiscalSpider
from Terms import FiscalTermsSpider


def getAuctions():
    bot = FiscalSpider(thread_number=10)

    # Cache the pages in the Database
    bot.setup_cache(
        backend="mongodb", port=27017, host="localhost", database="Auction_Data"
    )
    bot.run()
    # Write the urls with errors and types of errors
    # In .txt file
    # New error on the new row
    bot.errorsDf.to_csv("Errors/Fails.csv", mode="a")


def getTerms():

    termBot = FiscalTermsSpider(thread_number=10)
    # Intialize database for cache
    termBot.setup_cache(
        backend="mongodb", port=27017, host="localhost", database="Source"
    )
    termBot.run()
    # write terms dataset in .csv in the current folder:
    termBot.df.to_csv("Data/terms.csv", mode="a")
    termBot.errorsDf.to_csv("Errors/TermsFails.csv", mode="a")


# Initialize and activate the scrapping bots:
if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    # getAuctions()
    getTerms()
