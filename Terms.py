import pandas as pd
from grab import Grab
from urllib.parse import urlparse
from grab.spider import Spider, Task
from pymongo import MongoClient


class FiscalTermsSpider(Spider):
    initial_urls = [
        "https://auctions.grantstreet.com/auctions/results"
    ]  # <-That's a home page

    def addError(self, year, reason, url, aucType):
        data = {"year": year, reason: 1, "url": url, "type": aucType}
        self.errorsDf = self.errorsDf.append(data, ignore_index=True)

    def prepare(self):
        self.errorsDf = pd.DataFrame(columns=["year", "url", "type"])
        self.auctionTypes = {"Bond": 1, "Note": 2}
        # The same as before:
        self.df = pd.DataFrame()
        self.url = "https://auctions.grantstreet.com/auctions/results"

    # Bot checks all the years 1997-2021 (the same as before):
    def task_initial(self, grab, task):
        years = range(1997, 2021)
        for year in years:
            grab.setup(url=self.url, post={"searching": 1, "from": year})
            yield Task("years", grab=grab, year=year)

    # Takes from the page all the links for other sites with tables:
    # (The same as before)
    def task_years(self, grab, task):
        for aucType, value in self.auctionTypes.items():
            links = grab.doc.select(
                '//table[@id="panel_%s_table"]//a[contains(@href,"html")]/@href'
                % (value)
            )
            for link in links:
                yield Task("table", url=link.text(), aucType=aucType, year=task.year)

    def task_table_fallback(self, task):
        self.addError(task.year, "Connection Faild", task.url, task.aucType)

    # Finds Terms tables on the other sites and checks them:
    def task_table(self, grab, task):
        root = urlparse(task.url).hostname

        try:
            term_url = grab.doc.select(
                '//script[contains(text(),"open_terms()")]'
            ).text()
            term_url = term_url.split("window.open(")[1].split('"')[1]
            yield Task(
                "term",
                url="http://" + root + term_url,
                lasturl=task.url,
                year=task.year,
                aucType=task.aucType,
            )
        except:
            # Dealing with common error (Terms' page formating)
            try:
                term_url = grab.doc.select(
                    '//a[contains(text(), "Terms")]/@href'
                ).text()
                yield Task(
                    "term",
                    url="http://" + root + term_url,
                    lasturl=task.url,
                    year=task.year,
                    aucType=task.aucType,
                )
            except Exception as e:
                try:
                    # Dealing with common error (there is no direct link on Terms) -
                    # - Terms can be found through "Selections" section:
                    selection_url = grab.doc.select(
                        '//a[contains(text(), "Selections")]//@href'
                    ).text()
                    root = urlparse(task.url).hostname
                    # use task_selection function which is below for this url:
                    yield Task(
                        "selection",
                        url="http://" + root + selection_url,
                        lasturl=task.url,
                        year=task.year,
                        aucType=task.aucType,
                    )
                except Exception as e:
                    self.addError(
                        task.year,
                        getattr(e, "message", str(e)) + " INFO",
                        task.url,
                        task.aucType,
                    )

    def task_selection_fallback(self, task):
        self.addError(task.year, "Connection Faild", task.url, task.aucType)

    # This function is for "indirect terms":
    def task_selection(self, grab, task):
        try:
            # Prepare the terms url:
            term_url = grab.doc.select('//input[@value="View Terms"]/@onclick').text()
            term_url = term_url.split("'")[3]
            root = urlparse(task.url).hostname
            # use task_term function for this url:
            yield Task(
                "term",
                url="http://" + root + term_url,
                lasturl=task.lasturl,
                year=task.year,
                aucType=task.aucType,
            )
        except Exception as e:
            self.addError(
                task.year,
                getattr(e, "message", str(e)) + " INFO",
                task.url,
                task.aucType,
            )

    def task_term_fallback(self, task):
        self.addError(task.year, "Connection Faild", task.url, task.aucType)

    # Final part: takes the data and writes it down:
    def task_term(self, grab, task):
        new_df = pd.DataFrame(columns=[0, 1])
        try:
            for i in range(1, 5):
                # Terms table consists of 5 parts hence we need loop:
                table = grab.doc.select('//table[@cellpadding="1"][%s]' % i)
                df = pd.read_html(table.html())[1]
                # Attach new part for the previous table:
                new_df = new_df.append(df, ignore_index=True)
            new_df = new_df.T
            new_df.columns = new_df.iloc[0]
            new_df = new_df.drop(df.index[0])
            new_df = new_df.dropna(axis=0, how="all")
            new_df["url"] = task.lasturl
            new_df["terms_url"] = task.url
            new_df = new_df.loc[:, ~new_df.columns.duplicated()]
            # Attach it to the dataset:
            self.df = self.df.append(new_df)
        except Exception as e:
            self.addError(
                task.year,
                getattr(e, "message", str(e)) + " INFO",
                task.url,
                task.aucType,
            )
