import csv
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
 
 
#this function inputs the lda doc-topic matrix and modifies the topic
#columns based on manual review
def manual_lda(numRev,numTopics):
  
    #import pandas data frame from csv
    df = pd.read_csv('~/projects/def-mcorrito/mcorrito/gd_contractors/output/' + 'lda_final_' + str(numRev) + '_' + str(numTopics) + '_annual_agg.csv',sep=',',header=None,skiprows=[0])

    #remove ids
    ids = df.drop(df.columns[4:len(df.columns)],axis=1)

    ids = ids.drop(ids.columns[[0]],axis=1)
    ids.columns = ['orgid','year','word_count'] 
    
    df = df.drop(df.columns[[0,1,2,3]],axis=1)

    df.columns = range(df.shape[1])
         
    #combine certain topics
    df['24_76'] = df[[24,76]].sum(axis=1)
    df['19_35'] = df[[19,35]].sum(axis=1)
    df['54_56_21'] = df[[54,56,21]].sum(axis=1)
    df['14_61'] = df[[14,61]].sum(axis=1)
    df['16_74'] = df[[16,74]].sum(axis=1)
    df['30_52'] = df[[30,52]].sum(axis=1)
    df['2_17_25_42_45'] = df[[2,17,25,42,45]].sum(axis=1)
    df['8_39'] = df[[8,39]].sum(axis=1)        
    df['68_70_97'] = df[[68,70,97]].sum(axis=1)        
    df['41_80'] = df[[41,80]].sum(axis=1)        
    df['82_94'] = df[[82,94]].sum(axis=1)        
    df['1_59'] = df[[1,59]].sum(axis=1)            

    #create columns for the culture categories
    df['bureaucracy'] = df[29]
    df['community'] = df[60]
    df['procedural'] = df[33]
    df['diversity'] = df[72]
    df['teamExcellence'] = df[57]
    df['familyOriented'] = df[37]
    df['friendly'] = df['16_74']
    df['serenity'] = df[55]
    df['collegial'] = df['30_52']
    df['party'] = df[43]
    df['hierarchy'] = df['41_80']
    df['sharing'] = df[32]
    df['politics'] = df[87]
    df['stress'] = df['82_94']
    df['supportive'] = df[85]
    df['teamwork'] = df[49]
    df['hostility'] = df['1_59']
    df['fear'] = df[93]
    df['gossip'] = df[99]
        
    #remove certain topics
    df = df.drop(df.columns[[5,7,9,18,23,27,31,34,44,47,48,63,69,73,89,90,96,98,24,76,19,35,54,56,21,14,61,16,74,30,52,2,17,25,42,45,8,39,68,70,97,41,80,82,94,1,59]],axis=1)    

    #keep the culture topics
    df = df[['bureaucracy',
             'community',
             'procedural',
             'diversity',
             'teamExcellence',
             'familyOriented',
             'friendly',
             'serenity',
             'collegial',
             'party',
             'hierarchy',
             'sharing',
             'politics',
             'stress',
             'supportive',
             'teamwork',
             'hostility',
             'fear',
             'gossip']]
    
    #add IDs back on
    df = pd.concat([ids,df],axis=1,ignore_index=False)

    #df.columns = range(df.shape[1])

    #save as csv
    df.to_csv('~/projects/def-mcorrito/mcorrito/gd_contractors/output/' + 'lda_final_' + str(numRev) + '_' + str(numTopics) + '_manual' + '.csv')
 
 
 
   
manual_lda(4000,100)
 
 
