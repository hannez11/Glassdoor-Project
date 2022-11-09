import csv
import pandas as pd
import numpy as np
#from sklearn.metrics import pairwise_distances
#from sklearn.preprocessing import normalize
import math

from scipy.stats.kde import gaussian_kde
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#this is Govind's Jensen Shannon suite of functions
def jensen_shannon(p, q):
    pq = (p + q) / 2.0
    a = 0.5 * kl(p, pq)
    b = 0.5 * kl(q, pq)
    return np.sqrt(a + b)

def kl(p, q):
    return np.sum(p * safelog2(p/q))

def safelog2(x):
    with np.errstate(divide='ignore'):
        x = np.log2(x)
        x[np.isinf(x)] = 0.0 
        x[np.isnan(x)] = 0.0
        return x


#this function will compute mean JS similarity for reviews within org/quarter
def JS(numRev,numTopics,minWords):

    #create dataframe of non-contractor reviewIDs
    nocontract = pd.read_csv('/ifs/gsb/mcorrito/gd_contractors/data/' + 'noncontract_reviewIDs_annual.csv',sep=',')

    #open csv output file
    outFile = open('/ifs/gsb/mcorrito/gd_contractors/data/' + str(numRev) + '_' + str(numTopics) + '_JS_' + str(minWords) + 'nocontract_annual.csv','w')
    header = ['orgid','year','JS','num_reviews']
    write = csv.writer(outFile)
    write.writerow(header)
        
    #get tuple set of org/year in doc-term matrix to loop through 
    records = set()
    with open('/ifs/gsb/mcorrito/gd_contractors/data/' + 'top_unigrams_annual_' + str(numRev) + '.csv', 'r') as csvfile:
        read = csv.reader(csvfile,delimiter=",")
        next(read)
        for row in read:
            org = row[0]
            year = row[2]
            records.add((org,year))
    csvfile.close()

    #initialize JS container
    JS = []

    counter = 0

    #import pandas data frame from csv
    df = pd.read_csv('/ifs/gsb/mcorrito/gd_contractors/output/' + 'lda_final_' + str(numRev) + '_' + str(numTopics) + '_annual.csv',sep=',',header=None,skiprows=[0])

    #restrict to min number of words/review
    df = df[df[4] >= minWords]

    #drop contractor reviewids through merge
    df = df.rename(columns={ df.columns[2]: "reviewid" })

    print(len(df))

    df = pd.merge(df,nocontract,on='reviewid',how='inner')

    print(len(df))

    #build a matrix for each org/year
    for record in records:

        counter += 1

        #subset org/year specific matrix
        subset = df[(df[1]==float(record[0])) & (df[3]==float(record[1]))]

        #delete id columns
        subset = subset.drop(subset.columns[[0,1,2,3,4]],axis=1)

        #get num reviews
        num_reviews = subset.shape[0]
        
        print(subset.shape)
        print(len(records))
        print(counter)

        subset = subset.as_matrix()

        n = int(subset.shape[0])

        #compute JS scores across all reviews
        JS_mat = np.zeros((n, n))

        for i in xrange(0, n):
            for j in xrange(0, n):
                if i > j:
                    a = subset[i,]
                    b = subset[j,]

                    js = jensen_shannon(a,b)
                    if np.isfinite(js):
                        JS_mat[i,j] = js

        #calculate mean JS for all i,j review pairs
        lower = np.tril(JS_mat,-1)
        js = np.mean(lower[np.nonzero(lower)])

        print(js)
                
        #append ids and scores to CSV
        write.writerow([record[0],record[1],js,num_reviews])

                
    outFile.close()



  
JS(4000,500,minWords=5)





