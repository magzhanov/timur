#in cmd:
#pip install pandas
#pip install grab
#pip install --upgrade lxml
#pip install pycurl
#MongoClient (https://docs.mongodb.com/manual/installation/)

import requests
import logging
import requests
import json
import pandas as pd
from pprint import pprint
from grab.spider import Spider, Task
from pymongo import MongoClient

#1. Auction_Data bot (takes the main info)

# Creating a class for scrapping bot:

class FiscalSpider(Spider):
    # List of initial tasks
    # For each URL in this list the Task object will be created
    initial_urls = [
        "https://auctions.grantstreet.com/auctions/results"
    ]  #<-That's a home page

    def prepare(self):
        # Prepare the file handler to save results (e.g. Mongo Client).
        # The method `prepare` is called one time before the
        # spider has started working
        self.client = MongoClient(host="localhost", port=27017)
        
        #Set a name of database in the Mongo Client
        self.db = self.client["Auction_Data"]

        #Set a name of collection in the Mongo Client
        self.collection = self.db["Auction_Data"]

        #Define self.url
        self.url = "https://auctions.grantstreet.com/auctions/results"

        #Set error counter, error urls and types of errors
        self.errors = 0
        self.errors_urls = list()
        self.table_errors = set()

    # Bot checks all the years 1997-2021:
    def task_initial(self, grab, task):
        years = range(1997, 2021)

        # For each year it creates a new task 
        
        for year in years:
            
            #gets a page with POST request for a given year
            #(POST request for the page with a given year
            #can be found: F12->Network->results->Form Data)
            
            grab.setup(url=self.url, post={"searching": 1, "from": year})

            #use function task_years for this page:
            yield Task("years", grab=grab, year=year)

    # Takes from the page all the links for other sites with tables
    def task_years(self, grab, task):
        links = grab.doc.select(
            '//table[@class="pma_results"]//a[contains(@href,"html")]/@href'
        )
        for link in links:
            try:
                #use task_table function which is below for the link
                yield Task("table", url=link.text())
            except:
                #if error occurs then add this link to the error list
                self.errors_urls.append(link.text())

    # Finds tables on the other sites and checks them:
    def task_table(self, grab, task):
        #Takes main table (info_table) with auction's data:
        try:
            info_table = grab.doc.select('//table[@border="1"]')
            df = pd.read_html(info_table.html())[0]
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
            df = df.dropna(axis=0, how="all")
        except:
            try:
                #Dealing with common error (table's formating -
                # - there can be no borders in the table):
                info_table = grab.doc.select('//table[@cellpadding="3"]')
                df = pd.read_html(info_table.html())[0]
                df.columns = df.iloc[1]
                df = df.drop(df.index[0:2])
                df = df.dropna(axis=0, how="all")
            except Exception as e:
                #write down if error occurs:
                self.table_errors.add(task.url + " " + getattr(e, "message", str(e)))
                
        #Takes table with auction's date (time_table):
        try:
            time_table = grab.doc.select('//table[@cellspacing="2"]')
            time_df = pd.read_html(time_table.html())[0]
            dtime = pd.to_datetime(
                time_df[0][0], format="Auction\xa0Date \t%a.,\xa0%b\xa0%d,\xa0%Y"
            )
            date = dtime.strftime("%m/%d/%Y")
            #Scrap Auction type:
            aucType = time_df[4][0].split("Type")[1]
        except:
            #Dealing with common error (time table's formating):
            try:
                time_table = grab.doc.select('//table[@width="600"]')
                time_df = pd.read_html(time_table[2].html())[0]
                dtime = pd.to_datetime(time_df[0][1], format="%a.,\xa0%b\xa0%d,\xa0%Y")
                date = dtime.strftime("%m/%d/%Y")
                #Scrap Auction type:
                aucType = time_df[4][0].split("Type")[1]
            except Exception as e:
                self.table_errors.add(task.url + " " + getattr(e, "message", str(e)))

        # Take main table (info_table) and go through all the rows
        # Write down the data from these rows in the database
        # There can be different names of columns - better to discuss it together!
        # E.g. "Gross Interest" / "Total Interest" etc. 
        try:
            for index, row in df.iterrows():
                bidder = row["Bidder"]

                try:
                    firm = row["Firm"]
                except:
                    firm = None
                time = row["Time"]
                try:
                    gross_interest = row["Gross Interest"]
                except:
                    gross_interest = None
                try:
                    total_interest = row["Total Interest"]
                except:
                    total_interest = None

                try:
                    discount = row[row.index.str.contains("Discount", na=False)].values[
                        0
                    ]
                except:
                    discount = None
                try:
                    tic = row["TIC"]
                except:
                    tic = None
                try:
                    nic = row["NIC"]
                except:
                    nic = None
                try:
                    coupon = row["Coupon"]
                except:
                    coupon = None

                try:
                    principal = row["Principal"]
                except:
                    principal = None

                try:
                    premium = row["Premium"]
                except:
                    premium = None

                #Add data in the database: 
                self.collection.update_one(
                    {"date": date + " " + bidder},
                    {
                        "$set": {
                            "Bidder": bidder,
                            "Firm": firm,
                            "NIC": nic,
                            "TIC": tic,
                            "url": task.url,
                            "Gross Interest": gross_interest,
                            "Total Interest": total_interest,
                            "Premium": premium,
                            "Principal": principal,
                            "Coupon": coupon,
                            "Discount/Premium": discount,
                            "Time": time,
                            "Date": date,
                        }
                    },
                    upsert=True,
                )
        except:
            #if error occurs write it down:
            ++self.errors

# Initialize and activate the scrapping bot:

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = FiscalSpider(thread_number=10)

    #Cache the pages in the Database
    bot.setup_cache(
        backend="mongodb", port=27017, host="localhost", database="Auction_Data"
    )  
    bot.run()
    #Write the urls with errors and types of errors
    #In .txt file
    #New error on the new row
    
    with open("Failed.txt", "w") as file:
        for error in bot.table_errors:
            file.write(error + "\n")
            

#2. Terms bot (takes the Terms)

class FiscalTermsSpider(Spider):
    initial_urls = [
        "https://auctions.grantstreet.com/auctions/results"
    ]  #<-That's a home page

    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        #we will need this for taking all the pages in cache:
        self.db = self.client["Terms_Data"]
        self.collection = self.db["Terms_Data"]
        #The same as before:
        self.df = pd.DataFrame()
        self.url = "https://auctions.grantstreet.com/auctions/results"
        self.errors = 0
        self.table_error = set()
        self.term_url_error = set()
        self.term_table_error = set()

    # Bot checks all the years 1997-2021 (the same as before):
    def task_initial(self, grab, task):
        years = range(1997, 2021)
        for year in years:
            grab.setup(url=self.url, post={"searching": 1, "from": year})
            yield Task("years", grab=grab, year=year)

    # Takes from the page all the links for other sites with tables:
    # (The same as before)
    def task_years(self, grab, task):
        links = grab.doc.select(
            '//table[@class="pma_results"]//a[contains(@href,"html")]/@href'
        )
        for link in links:
            try:
                yield Task("table", url=link.text())
            except Exception as e:
                self.table_errors.add(task.url + " " + getattr(e, "message", str(e)))

    # Finds Terms tables on the other sites and checks them:
    def task_table(self, grab, task):
        try:
            term_url = grab.doc.select(
                '//script[contains(text(),"open_terms()")]'
            ).text()
            term_url = term_url.split("window.open(")[1].split('"')[1]
        except:
            #Dealing with common error (Terms' page formating)
            try:
                term_url = grab.doc.select(
                    '//a[contains(text(), "Terms")]/@href'
                ).text()
            except Exception as e:
                try:
                    #Dealing with common error (there is no direct link on Terms) -
                    # - Terms can be found through "Selections" section: 
                    selection_url = grab.doc.select(
                        '//a[contains(text(), "Selections")]//@href'
                    ).text()
                    root = urlparse(task.url).hostname
                    #use task_selection function which is below for this url:
                    yield Task(
                        "selection",
                        url="http://" + root + selection_url,
                        lasturl=task.url,
                    )
                except Exception as e:
                    self.term_url_error.add(
                        task.url + " " + getattr(e, "message", str(e))
                    )
                    
        #Just a useful part of auction's url: 
        root = urlparse(task.url).hostname
        
        try:
            #use task_term function for this url: 
            yield Task("term", url="http://" + root + term_url, lasturl=task.url)
        except Exception as e:
            self.table_error.add(task.url + " " + getattr(e, "message", str(e)))
            
    #This function is for "indirect terms":
    def task_selection(self, grab, task):
        try:
            #Prepare the terms url:
            term_url = grab.doc.select('//input[@value="View Terms"]/@onclick').text()
            term_url = term_url.split("'")[3]
            root = urlparse(task.url).hostname
            #use task_term function for this url:
            yield Task("term", url="http://" + root + term_url, lasturl=task.lasturl)
        except Exception as e:
            self.term_url_error.add(task.url + " " + getattr(e, "message", str(e)))

    #Final part: takes the data and writes it down: 
    def task_term(self, grab, task):
        new_df = pd.DataFrame(columns=[0, 1])
        try:
            for i in range(1, 5):
                #Terms table consists of 5 parts hence we need loop:
                table = grab.doc.select('//table[@cellpadding="1"][%s]' % i)
                df = pd.read_html(table.html())[1]
                #Attach new part for the previous table:
                new_df = new_df.append(df, ignore_index=True)
            new_df = new_df.T
            new_df.columns = new_df.iloc[0]
            new_df = new_df.drop(df.index[0])
            new_df = new_df.dropna(axis=0, how="all")
            new_df["url"] = task.lasturl
            new_df["terms_url"] = task.url
            new_df = new_df.loc[:, ~new_df.columns.duplicated()]
            #Attach it to the dataset: 
            self.df = self.df.append(new_df)
        except Exception as e:
            self.term_table_error.add(task.url + " " + getattr(e, "message", str(e)))


# Initialize and activate the scrapping bot:
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    termBot = FiscalTermsSpider(thread_number=10)
    termBot.setup_cache(
        backend="mongodb", port=27017, host="localhost", database="Terms_Data"
    )
    termBot.run()
    #write terms dataset in .csv in the current folder:
    termBot.df.to_csv("Data/terms.csv", mode="a")
    #write errors in .txt in the current folder:
    with open("Term_urls_failed.txt", "w") as file:
        for error in termBot.term_url_error:
            file.write(error + "\n")
    with open("Term_table_failed.txt", "w") as file:
        for error in termBot.term_table_error:
            file.write(error + "\n")
