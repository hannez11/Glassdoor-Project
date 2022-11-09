#this file creates a review X unigram document term matrix from the GD
#contractor dictionary
import csv, json, nltk, re, urllib2

#function that removes non-words, lowercase, and tokenizes
def token(txt):
    txt = re.sub("'","",txt)
    txt = re.sub("\W+"," ",txt)
    txt = re.sub('\d+'," ",txt)
    txt = txt.lower()
    txt = nltk.word_tokenize(txt)
    return txt
                        

#define function that creates doc-term matrix for top x number of words
def docTerm(dict,minCount,field):
     
    #import stemmer
    porter = nltk.stem.PorterStemmer()

    #prepare stop words list
    stopWords = urllib2.urlopen("http://web.stanford.edu/~mcorrito/stop_words.html").readlines()
    stopWords = {x.strip("\n") for x in stopWords}
    stopWords = {x.strip("\r") for x in stopWords}
    stopWords = {re.sub("'","",x) for x in stopWords}
    stopWords = {re.sub("\W+"," ",x) for x in stopWords}
    stopWords = {re.sub('\d+'," ",x) for x in stopWords}

    #load dictionary
    with open('/ifs/gsb/mcorrito/gd_contractors/data/' + dict, 'r') as gd:
        data = json.load(gd)

    #initialize dictionary to record popularity of unigrams    
    unigrams = {}

    #identify total word count across reviews within org/quarter
    for reviewID in data.keys():

        #remove stop words, custom stop words, and stem
        tokens = [x for x in data[reviewID][field]['text'] if x not in stopWords]
        tokens = [porter.stem(w) for w in tokens]

        #track frequencies of unigrams in each review
        data[reviewID][field]['wordCount_uni'] = len(tokens)
        data[reviewID][field]['uniCount'] = {}
        
        for i in set(tokens):

            count = tokens.count(i)

            #for each unigram, add count to dictionary
            data[reviewID][field]['uniCount'][i] = count

            #add to unigrams dictionary
            if i in unigrams:
                unigrams[i] += count
            else:
                unigrams[i] = count

    #drop unigrams used less than X times
    for word in unigrams.keys():
        if unigrams[word] < minCount:
            del unigrams[word]
                
    topUnigrams = sorted(unigrams,key=unigrams.get,reverse=True)

    print("writing to csv")
                            
    #write org/review doc term matrix to csv
    headerUni = ["reviewid"] + topUnigrams 

    with open('/ifs/gsb/mcorrito/gd_contractors/' + "top_unigrams_" + field + '_' + str(minCount) + ".csv","w") as csvfile:
        writer = csv.writer(csvfile,delimiter=",",lineterminator='\n')
        writer.writerow(headerUni)

        for reviewID in data.keys():

            toWrite = [reviewID]

            for i in topUnigrams:
                if i in data[reviewID][field]['uniCount']:
                    toWrite.append(str(data[reviewID][field]['uniCount'][i]))
                else:
                    toWrite.append(str(0)) 

            writer.writerow(toWrite)
                            
    csvfile.close()    





            
docTerm('gd_contractors_dict',25,'pro')
print("done")
docTerm('gd_contractors_dict',25,'con')


                 

