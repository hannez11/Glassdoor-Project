import urllib
import requests
from bs4 import BeautifulSoup

firmname = "Cable One"
search.yahoo.com/search?p=site:glassdoor.com "Tesla" "Working at"&vc=en
query = f"site:glassdoor.com {firmname} 'Working at'".replace(' ', '+')
URL = f"https://www.bing.com/search?q={query}&setLang=en"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0"
headers = {"user-agent":USER_AGENT}
url = requests.get(URL, headers=headers)
soup = BeautifulSoup(url.content, "html.parser")

##check if bing returns a page
if soup.findAll("li", {"class":"b_algo"}):
    ##loop through all search entries
    for g in soup.findAll("li", {"class":"b_algo"}): #loop through every google search result (includes link, title, rating, body)

        links = soup.findAll("a")
        for link in links:
            print(link["href"])
        # link = g.find_all("div", class_="b_attribution").text
        # print(link)

        # link_and_title = g.find_all('a', limit=1) #get the link and title within current search result
        # print(link_and_title)
        # if link_and_title: #if link and title is found
        #     company_words = firmname.lower().split() #split the complete company name into separate strings
        #     link = link_and_title[0]['href'] #get the href of the first list item
        #     # title = g.find('h3').text #get the title
        #     print(f"company name: {company_words}")

        # rating_and_reviews = g.find_all('div', class_="b_sritem") #get the div displaying the rating and amount of reviews
        # print(f"ratings: {rating_and_reviews[0].text} \n")

        break