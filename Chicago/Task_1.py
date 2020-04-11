from bs4 import BeautifulSoup
import requests
import pandas as pd
from fuzzywuzzy import process

data = pd.DataFrame(columns=["Symbol", "Name", "Link", "Score", "Found Name"])
response = requests.get("https://en.wikipedia.org/wiki/S&P_100")

soup = BeautifulSoup(response.text, "html.parser")
table = soup.select(".wikitable")[1]

df = pd.read_csv("names_gvkeys.csv")
variants = list(df.conm)
wikiRoot = "https://en.wikipedia.org"
for row in table.tbody.find_all("tr")[1:]:
    td = row.find_all("td")
    symbol = td[0].text.replace("\n", "")
    companyName = td[1].a.text
    companyLink = td[1].a["href"]
    score = process.extractOne(companyName, variants)
    data = data.append(
        {
            "Symbol": symbol,
            "Name": companyName,
            "Link": wikiRoot + companyLink,
            "Score": score[1],
            "Found Name": score[0],
        },
        ignore_index=True,
    )

data.to_csv("Task_1.csv", mode="a")
