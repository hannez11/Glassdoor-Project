#this file creates an org/review document term matrix 
import csv, json, nltk, re, urllib2

#define function that creates doc-term matrix for top x number of words
def docTerm(dict,numWords):
     
    #import stemmer
    porter = nltk.stem.PorterStemmer()

    #prepare stop words list
    stopWords = urllib2.urlopen("http://web.stanford.edu/~mcorrito/stop_words.html").readlines()
    stopWords = {x.strip("\n") for x in stopWords}
    stopWords = {porter.stem(w) for w in stopWords}

    #load dictionary
    with open('/ifs/gsb/mcorrito/gd_contractors/data/' + dict, 'r') as gd:
        data = json.load(gd)

    #initialize dictionary to record popularity of unigrams (unigrams that
    #appear in a high propostion of the total words across an organization)    
    unigrams = {}

    #identify total word count across reviews within org/quarter
    for yr in data.keys():
        for orgID in data[yr].keys():
            for reviewID in data[yr][orgID].keys():

                txt = {}                    

                for field in data[yr][orgID][reviewID].keys():
                    txt[field] = []

                    if data[yr][orgID][reviewID][field]['wordCount'] > 0:

                        txt[field] = data[yr][orgID][reviewID][field]['text']

                allTxt = txt['pro'] + txt['con'] 

                #remove stop words, custom stop words, and stem
                tokens = [porter.stem(w) for w in allTxt]
                tokens = [x for x in tokens if x not in stopWords]

                #track frequencies of unigrams in each review
                data[yr][orgID][reviewID]['all'] = {}
                data[yr][orgID][reviewID]['all']['wordCount'] = len(tokens)


                data[yr][orgID][reviewID]['all']['uniCount'] = {}

                for i in set(tokens):

                    count = tokens.count(i)

                    #for each unigram, act count to dict
                    data[yr][orgID][reviewID]['all']['uniCount'][i] = count

                    #add to unigrams dictionary
                    if i in unigrams:
                        unigrams[i] += count
                    else:
                        unigrams[i] = count

                            
    #get words from the culture model
    cultWords = open('/ifs/gsb/mcorrito/duality/' + 'top_unigrams_phrase_' + str(numWords) + '_ref_pruned.csv','r')
    read = csv.reader(cultWords,delimiter=",")
    topUnigrams = None
    for row in read:
        topUnigrams = row

    #write org/review doc term matrix to csv
    headerUni = ["orgid"] + ["reviewid"] + ["year"] + topUnigrams 

    with open('/ifs/gsb/mcorrito/gd_contractors/data/' + "top_unigrams_annual_" + str(numWords) + ".csv","w") as csvfile:
        writer = csv.writer(csvfile,delimiter=",",lineterminator='\n')
        writer.writerow(headerUni)

        for yr in data.keys():
            for orgID in data[yr].keys():
                for reviewID in data[yr][orgID].keys():

                    if data[yr][orgID][reviewID]['all']['wordCount'] > 0:
                        toWrite = [orgID,reviewID,yr]

                        for i in topUnigrams:
                            if i in data[yr][orgID][reviewID]['all']['uniCount']:
                                toWrite.append(str(data[yr][orgID][reviewID]['all']['uniCount'][i]))
                            else:
                                toWrite.append(str(0)) 

                        writer.writerow(toWrite)
                            
    csvfile.close()    





            
docTerm('gd_dict_annual_cult',4000)


                 

