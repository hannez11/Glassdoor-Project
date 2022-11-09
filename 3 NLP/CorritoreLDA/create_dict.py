#this file adds the glassdoor text for the contractor reviews into a dictionary and
#saves to a json object  
import csv, nltk, re, json
from itertools import islice 

#function that removes non-words, lowercase, and tokenizes
def token(txt):
    txt = re.sub("'","",txt)
    txt = re.sub("\W+"," ",txt)
    txt = re.sub('\d+'," ",txt)
    txt = txt.lower()
    txt = nltk.word_tokenize(txt)
    return txt


#add text to dictionary 
gd_dict = {}

with open('/tmp/' + 'extract.csv', 'r') as csvfile:
    read = csv.reader(csvfile, delimiter = ',')

    next(read)
    
    for row in read:

        try:
        
            reviewID = row[0]
            pro = row[1]
            con = row[2]
            feedback = row[3]

            #call function to remove non-words, lowercase, and tokenize
            pro = token(pro)
            con = token(con)
            feedback = token(feedback)
            
            #add reviewid to dictionary
            gd_dict[reviewID] = {}

            #add text to dictionary and count words
            gd_dict[reviewID]['pro'] = {}
            gd_dict[reviewID]['pro']['text'] = pro
            gd_dict[reviewID]['pro']['wordCount'] = len(pro)

            gd_dict[reviewID]['con'] = {}
            gd_dict[reviewID]['con']['text'] = con
            gd_dict[reviewID]['con']['wordCount'] = len(con)

            gd_dict[reviewID]['feedback'] = {}
            gd_dict[reviewID]['feedback']['text'] = con
            gd_dict[reviewID]['feedback']['wordCount'] = len(con)
            
        except:
            print("Error")
                
#save dictionary as json object
with open('/ifs/gsb/mcorrito/gd_contractors/data/' + 'gd_contractors_dict', 'w') as gd:
    json.dump(gd_dict, gd)



            
                
            
