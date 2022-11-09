from multiprocessing import Process, Pool, current_process
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote
import random
import time
from pathlib import Path
from torpy.http.requests import TorRequests
from itertools import repeat
from fritzconnection import FritzConnection

# set relevant parameters at the very bottom

input = pd.read_csv(r"W:\019_Glassdoor\1 Data\1 Glassdoor Links\2 Eikon Sample\1004_Eikon_AcqAndTargets_9496deal_firms.csv", sep=";", decimal=",", encoding='unicode_escape')

class YahooScraperClass:
    def yahoo_scraper(self, df, sleeptime_max=1, tor_active="no"):
        try: # if operation fails, then return the current df
            firmname = df["full_name_cleaned"]
            print(f"Firm #{df.name + 1}: {firmname}")
            # use non_strict query
            query = f'site:glassdoor.com {firmname}'.replace(' ', '+') # non-strict
            # use strict query
            # query = f'site:glassdoor.com "{firmname}"'.replace(' ', '+') #putting {firmname} around "" might help. however, the query might also be too strict not returning any results
            URL = f"https://search.yahoo.com/search?p={query}&vc=en&pz=20"

            # shuffle through user agent list for bot prevention. use IP shuffling to prevent further bot detection
            time.sleep(round(random.uniform(0.25,sleeptime_max), 3))
            USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36 Edg/103.0.1264.71"
                        ]
            headers = {"user-agent":random.choice(USER_AGENTS)}

            ## use tor
            if tor_active=="yes":
                try:
                    url = self.sess.get(URL, headers=headers)
                except:
                    print("Error getting the URL with tor requests")
            ## use regular IP
            else:
                url = requests.get(URL, headers=headers)

            soup = BeautifulSoup(url.content, "html.parser")
            time.sleep(0.1) #to fully load the page

            ## scraper ##
            #############
            link_found = 0
            ## check if amount of ratings is displayed in at least of the search results. if so, grab this number and the parent div containing the link
            if soup.find("li", {"class":["tc", "bxz-bb"]}) and len(soup.find("li", {"class":["tc", "bxz-bb"]}).text)<15: #check length of the classes text, since class is not exclusively used for amount of ratings
                ## grab the review number
                review_amount = soup.find("li", {"class":["tc", "bxz-bb"]}) #only get the first item, since it appears to be most accurate. could be of the last search result
                try: #sometimes "Currency: EUR" is displayed instead of the review amount
                    review_amount_final = review_amount.text.split("(")[1][:-1] #"4/5 (3)"
                    if "K" in review_amount_final: # "5.7K" to 5700
                        review_amount_final = review_amount_final.replace(".", "")
                        review_amount_final = review_amount_final.replace("K", "00")
                    # print(f"review amount: {review_amount_final}")
                    df["review_amount"] = review_amount_final
                    ## grab the href from the parent of the same search result
                except:
                    df["review_amount"] = "" 
                review_parents = review_amount.find_parents("div")
                for link in review_parents:
                    if link.find("a") and "glassdoor" in link.find("a")["href"]:
                        alink = link.find("a")
                        link_found = 1
                        break
            ## if amount of ratings is not on page, then just grab the href
            elif link_found != 1:
                # print("review amount: none found")
                results = soup.findAll("h3")
                for link in results:
                    if link.find("a") and "glassdoor" in link.find("a")["href"]:
                        alink = link.find("a")
                        link_found = 1
                        break
                ## if neither link nor review amount was found
                if link_found != 1:
                    link_found = "no link found" #"no link found"
                    link_final = ""
                    print("no link found on first page")
            
            ## decode href link if link was found ##
            ########################################
            if link_found == 1:
                link_with_yahoo = unquote(alink["href"]) #unquote decodes URLs
                try: # cover weird cases in which an image link is retrieved
                    link_final = link_with_yahoo.split("RU=")[1].split("/RK=2")[0] #https://r.search.yahoo.com/_ylt=A0geK9dD9t9i2BIAtodXNyoA;_ylu=Y29sbwNiZjEEcG9zAzEEdnRpZAMEc2VjA3Ny/RV=2/RE=1658873541/RO=10/RU=https://www.glassdoor.com/Overview/Working-at-California-Micro-Devices-EI_IE1221.11,35.htm/RK=2/RS=Ss7CVR85Jhnt899NkWVSw9eSVUw-
                    if "/Working" in link_final:
                        # from https://www.glassdoor.com/Overview/Working-at-Tesla-EI_IE43129.11,16.htm
                        # to https://www.glassdoor.com/Reviews/Tesla-Reviews-E43129.htm
                        link_final = link_final.replace("/Overview/Working-at-", "/Reviews/") #shouldnt be an issue to replace the /Overview/ part with /Reviews/ even though the original Reviews link looks different
                        link_final = link_final.replace("EI_IE", "Reviews-E") 
                        link_final = link_final.split(".htm")[0][:-6] + ".htm"
                        link_found = "transformed overview link"
                    elif "/Jobs/" in link_final:
                        link_final = link_final.replace("/Jobs/", "/Reviews/")
                        link_final = link_final.replace("-Jobs-", "-Reviews-")
                        link_found = "transformed jobs link"
                    elif "/Salary/" in link_final:
                        link_final = link_final.replace("/Salary/", "/Reviews/")
                        link_final = link_final.replace("-Salaries-", "-Reviews-")
                        link_found = "transformed salary link"
                    elif "/Reviews/" in link_final:
                        link_found = "original review link"
                    else:
                    #elif "/job-listing/" or "/Job/" in link_final:
                        link_found = "wrong link"
                except:
                    link_final = link_with_yahoo
                    
            df["link_yahoo"] = link_final
            df["link_found"] = link_found
            print(link_final, "\n")
            return df
            
        except:
            print("Some error")
            return df

    def tor_yahoo_scraper(self, df_dynamic, startrow=0, endrow=5, tor="no", sleeptime_max=0.5, save_path=Path.home() / 'Desktop'):
            ## set parameters
            # tor: change to yes or no
            # sleeptime_max: use >2 with original ip (tor = "no"). around 1000 requests possible until temp. ip block

            endrow = endrow + 1 # otherwise stops at endrow-1

            print(f"Current process: {current_process().name}")
            start = time.time()

            ## create a copy of the input frame; keep old index as extra column
            df_dynamic = input[startrow:endrow].reset_index(drop=False)

            df_dynamic["link_yahoo"] = np.nan
            df_dynamic["review_amount"] = np.nan
            df_dynamic["link_found"] = np.nan

            ## with Tor-Gateway
            if tor == "yes":
                with TorRequests() as tor_requests:
                    print("Connecting to tor..")
                    with tor_requests.get_session() as self.sess: #sess pulls a request of the page with the tor connection. self is crucial in front of sess. otherwise wont work in the yahoo_scraper method
                        current_ip = self.sess.get("http://httpbin.org/ip").json()
                        print(f"Current IP:{current_ip}; sleeptime: {sleeptime_max}\n")
                        df_dynamic = df_dynamic.apply(self.yahoo_scraper, args=(sleeptime_max, "yes"), axis=1)
            ## without tor
            else:
                df_dynamic = df_dynamic.apply(self.yahoo_scraper, args=(sleeptime_max, "no"), axis=1)

            print(f"Scraping finished for {startrow} to {endrow}")
            
            ## save file with dynamic file name
            dir = Path(save_path)
            path_with_time = Path.joinpath(dir, f'{time.strftime("%m%d")}_MnA_Sample_Eikon_{startrow}_{endrow}.csv')
            df_dynamic.to_csv(path_with_time, sep=";", decimal=",", float_format='%.3f', index=False)
            print(f"{path_with_time} saved")

            end = time.time()
            duration = end - start
            print(f"Process {current_process().name} took {duration:.2f}s")

    def create_pool_args(self,
        first_row=0,
        lastrow=input.shape[0], 
        rows_per_pool=500,
        tor_active="no",
        sleeptime_max=0.5):

        startrows = [i for i in range (first_row,lastrow,rows_per_pool)]
        endrows = [i for i in range (first_row+rows_per_pool-1,lastrow,rows_per_pool)]

        dfs_multi_namelist = [f"df_mp{i}" for i in range(len(startrows))]
        dfs_multi = {name: pd.DataFrame() for name in dfs_multi_namelist} # create multiple empty dataframes

        self.pool_args = list(zip(dfs_multi_namelist, startrows, endrows, repeat(tor_active), repeat(sleeptime_max)))
        print(self.pool_args)

    def fritz_reconnect(self):
        fc = FritzConnection(address='192.168.178.1')
        fritz_ip_old = requests.get("http://httpbin.org/ip").json()
        print(f"Old IP:{fritz_ip_old}")
        fc.reconnect()  # get a new external ip from the provider
        time.sleep(10)
        fritz_ip_new = requests.get("http://httpbin.org/ip").json()
        print(f"New IP:{fritz_ip_new}")

    def start_scrape(self, fritz_reconnect = "no"):
        if __name__ == '__main__':
            #######################
            # set parameters here #
            #######################
            self.create_pool_args(
                first_row=6000,
                lastrow=6250,
                rows_per_pool=250,
                tor_active = "no",
                sleeptime_max=0.0)

            # Pool().starmap(self.tor_yahoo_scraper, self.pool_args)
            Pool().starmap(self.tor_yahoo_scraper, [('df_mp0', 8500, 8750, 'no', 0),])

            if fritz_reconnect == "yes":
                self.fritz_reconnect()
# execute #
YahooScraperClass().start_scrape(fritz_reconnect = "no")

# YahooScraperClass().fritz_reconnect()

# YahooScraperClass().create_pool_args(
#                 first_row=0,
#                 lastrow=1000,
#                 rows_per_pool=250,
#                 tor_active = "no",
#                 sleeptime_max=0.2)


