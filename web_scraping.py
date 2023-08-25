import requests
from bs4 import BeautifulSoup
import pandas as pd


url = "https://www.indiatoday.in/india"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')


def get_web_contents():
    Titles = []
    descriptions = []
    titles = soup.select('div.B1S3_content__wrap__9mSB6')  # Title data tag
    for x in titles:  # Get all news titles
        Titles.append(x.find('a')["title"])
        descriptions.extend(x.find('p').contents)
    return Titles, descriptions

def scraped_df():
    titles = pd.Series(get_web_contents()[0])
    descriptions = pd.Series(get_web_contents()[1])
    scraped_data = pd.DataFrame({'Title':titles, 'Description':descriptions})
    return scraped_data
# print(soup.select('div.B1S3_content__wrap__9mSB6')[0].find('a')["title"])
# print(scraped_data.iloc[0])