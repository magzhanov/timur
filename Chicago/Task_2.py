import nltk
import string
import re
from nltk.corpus import stopwords
from os import listdir
import pandas as pd


dirPath = "Risks/"

df = pd.DataFrame(columns=["Company Name", "Words count", "Competition Count"])

stopWords = set(stopwords.words("english"))

lemma = nltk.wordnet.WordNetLemmatizer()

patentDf = pd.DataFrame(
    columns=["Company Name", "Year", "Previous", "Patent word", "Following"]
)

for filePath in listdir(dirPath):
    file = open(dirPath + filePath, "r")

    riskText = ""

    for line in file:
        if (
            "|"
            and "OTHER KEY INFORMATION"
            and "Table of Contents" not in line
            and not line.isdigit()
        ):
            riskText = riskText + line.lower()

    riskText = riskText.translate(str.maketrans("", "", string.punctuation))
    company = filePath.split("_")[0]
    year = filePath.split("_")[1].split(".")[0]
    words = nltk.word_tokenize(riskText)
    countCompetition = words.count("competition")
    length = len(words)

    filtered_words = [word for word in words if word not in stopWords]
    for i, word in enumerate(filtered_words):
        if lemma.lemmatize(word) == "patent":
            patentDf = patentDf.append(
                {
                    "Company Name": company,
                    "Year": year,
                    "Previous": filtered_words[i - 1],
                    "Patent word": word,
                    "Following": filtered_words[i + 1],
                },
                ignore_index=True,
            )

    df = df.append(
        {
            "Company Name": company,
            "Words count": length,
            "Competition Count": countCompetition,
        },
        ignore_index=True,
    )

df.to_csv("Task_2_1.csv", mode="a")
patentDf.to_csv("Task_2_2.csv", mode="a")
