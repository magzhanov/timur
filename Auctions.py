import pandas as pd
from grab import Grab
from grab.spider import Spider, Task
from pymongo import MongoClient


class FiscalSpider(Spider):
    # List of initial tasks
    # For each URL in this list the Task object will be created
    initial_urls = [
        "https://auctions.grantstreet.com/auctions/results"
    ]  # <-That's a home page

    def addError(self, year, reason, url, aucType):
        data = {"year": year, reason: 1, "url": url, "type": aucType}
        self.errorsDf = self.errorsDf.append(data, ignore_index=True)

    def prepare(self):
        # Prepare the file handler to save results (e.g. Mongo Client).
        # The method `prepare` is called one time before the
        # spider has started working
        self.client = MongoClient(host="localhost", port=27017)

        # Set a name of database in the Mongo Client
        self.db = self.client["Auction_Data"]
        self.errorsDf = pd.DataFrame(columns=["year", "url", "type"])
        self.auctionTypes = {"Bond": 1, "Note": 2}

        # Set a name of collection in the Mongo Client
        self.collection = self.db["Auction_Data"]

        # Define self.url
        self.url = "https://auctions.grantstreet.com/auctions/results"

        # Set error counter, error urls and types of errors
        self.errors = 0
        self.errors_urls = list()
        self.table_errors = set()

    # Bot checks all the years 1997-2021:
    def task_initial(self, grab, task):
        years = range(1997, 2021)

        # For each year it creates a new task

        for year in years:

            # gets a page with POST request for a given year
            # (POST request for the page with a given year
            # can be found: F12->Network->results->Form Data)

            grab.setup(url=self.url, post={"searching": 1, "from": year})

            # use function task_years for this page:
            yield Task("years", grab=grab, year=year)

    # Takes from the page all the links for other sites with tables
    def task_years(self, grab, task):
        for aucType, value in self.auctionTypes.items():
            links = grab.doc.select(
                '//table[@id="panel_%s_table"]//a[contains(@href,"html")]/@href'
                % (value)
            )
            for link in links:
                yield Task("table", url=link.text(), aucType=aucType, year=task.year)

    def task_table_fallback(self, task):
        self.addError(task.year, "Connection Failed", task.url, task.aucType)

    # Finds tables on the other sites and checks them:
    def task_table(self, grab, task):
        # Takes main table (info_table) with auction's data:
        try:
            info_table = grab.doc.select('//table[@border="1"]')
            df = pd.read_html(info_table.html())[0]
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
            df = df.dropna(axis=0, how="all")
        except:
            try:
                # Dealing with common error (table's formating -
                # - there can be no borders in the table):
                try:
                    info_table = grab.doc.select('//table[@cellpadding="3"]')
                except:
                    info_table = grab.doc.select('//table[@cellpadding="5"]')
                df = pd.read_html(info_table.html())[0]
                df.columns = df.iloc[1]
                df = df.drop(df.index[0:2])
                df = df.dropna(axis=0, how="all")
            except Exception as e:
                # write down if error occurs:
                self.addError(
                    task.year,
                    getattr(e, "message", str(e)) + " INFO",
                    task.url,
                    task.aucType,
                )

        # Takes table with auction's date (time_table):
        try:
            time_table = grab.doc.select('//table[@cellspacing="2"]')
            time_df = pd.read_html(time_table.html())[0]
            dtime = pd.to_datetime(
                time_df[0][0], format="Auction\xa0Date \t%a.,\xa0%b\xa0%d,\xa0%Y"
            )
            date = dtime.strftime("%m/%d/%Y")
            # Scrap Auction type:
            aucType = time_df[4][0].split("Type")[1]
        except:
            # Dealing with common error (time table's formating):
            try:
                time_table = grab.doc.select('//table[@width="600"]')
                time_df = pd.read_html(time_table[2].html())[0]
                dtime = pd.to_datetime(time_df[0][1], format="%a.,\xa0%b\xa0%d,\xa0%Y")
                date = dtime.strftime("%m/%d/%Y")
                # Scrap Auction type:
                aucType = time_df[4][0].split("Type")[1]
            except Exception as e:
                self.addError(
                    task.year,
                    getattr(e, "message", str(e)) + " TIME",
                    task.url,
                    task.aucType,
                )

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

                # Add data in the database:
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
                            "Type": task.aucType,
                        }
                    },
                    upsert=True,
                )
        except Exception as e:
            self.addError(
                task.year, getattr(e, "message", str(e)), task.url, task.aucType
            )
