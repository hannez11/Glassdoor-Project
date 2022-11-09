#this file adds the glassdoor text for each review into a dictionary and
#saves to a json object  
import csv, nltk, re, json, sys
from itertools import islice 

csv.field_size_limit(sys.maxsize)

#function that adds year to dictionary
def addYear(year,dic):
    #create year subdictionary if it doesn't exist        
    if year in dic.keys():
        return dic
    else:
        dic[year] = {}
        return dic 

#function that adds orgID to dictionary
def addOrgID(year,orgID,dic):
    #create org subdictionary if it doesn't exist
    if orgID in dic[year].keys():
        return dic
    else:
        dic[year][orgID] = {}
        return dic

#function that adds reviewID to dictionary
def addReviewID(year,orgID,reviewID,dic):
    #create org/review subdictionary if it doesn't exist
    if reviewID in dic[year][orgID].keys():
        return dic
    else:
        dic[year][orgID][reviewID] = {}
        return dic

#function that removes non-words, lowercase, and tokenizes
def token(txt):
    txt = re.sub("'","",txt)
    txt = re.sub("\W+"," ",txt)
    txt = re.sub('\d+'," ",txt)
    txt = txt.lower()
    txt = nltk.word_tokenize(txt)
    return txt


#read in master reviewIDs and assign to a set
masterIDs = set()
with open('/ifs/gsb/mcorrito/gd_contractors/data/' + 'master_orgIDs_annual.csv', 'r') as csvfile:
    read = csv.reader(csvfile, delimiter = ',')

    for row in read:
        masterIDs.add(row[0])
csvfile.close()


#the columns I need are 1 (reviewID), 2 (orgID), 3 (timestamp), and 9-10 (pros, cons)
#will only create dictionary entries for orgs I need by checking against
#set and for valid review IDs
gd_dict = {}

with open('/tmp/' + 'extract.csv', 'r') as csvfile:
    read = csv.reader(csvfile, delimiter = ',')

    next(read)
    
    for row in read:
        reviewID = row[0]

        #check if a master review ID
        if reviewID in masterIDs:

            #create date vars
            timestamp = (row[2].split(" "))[0]
            timestamp = timestamp.split("-")
            year = int(timestamp[0])

            orgID = row[1]                                      
            pro = row[8]
            con = row[9]
                
            #call function to remove non-words, lowercase, and tokenize
            pro = token(pro)
            con = token(con)

            #add year,month,org,reviewid to dictionary
            gd_dict = addYear(year,gd_dict)
            gd_dict = addOrgID(year,orgID,gd_dict)
            gd_dict = addReviewID(year,orgID,reviewID,gd_dict)

            #add text to dictionary and count words
            gd_dict[year][orgID][reviewID]['pro'] = {}
            gd_dict[year][orgID][reviewID]['pro']['text'] = pro
            gd_dict[year][orgID][reviewID]['pro']['wordCount'] = len(pro)
                
            gd_dict[year][orgID][reviewID]['con'] = {}
            gd_dict[year][orgID][reviewID]['con']['text'] = con
            gd_dict[year][orgID][reviewID]['con']['wordCount'] = len(con)


#save dictionary as json object
with open('/ifs/gsb/mcorrito/gd_contractors/data/' + 'gd_dict_annual_cult', 'w') as gd:
    json.dump(gd_dict, gd)



            
                
            
