#this file creates cultural ratings for org cultures along
#bureaucracy,"clique", community, family, team, and hostility

import csv
import pandas as pd
import numpy as np

#load complete doc-term matrix
df = pd.read_csv('~/projects/def-mcorrito/mcorrito/gd_contractors/data/' + 'top_unigrams_annual_4000' + '.csv',sep=',',low_memory=False)

#calculate review word count 
wordCount = np.sum(df.drop(df.columns[[0,1,2]],axis=1),axis=1)
df['wordCount'] = wordCount

#drop reviewid column
df = df.drop(['reviewid'], axis=1)

#sum rows within org/years
df = df.groupby([df['orgid'],df['year']]).sum().reset_index()

#create culture columns 
df['bureaucratic'] = df['polit']+df['process']+df['polici']+df['rule']+df['procedur']+df['guidelin']+df['regul']+df['law']+df['bureaucraci']+df['bureaucrat']+df['red']+df['tape']+df['hierarch']+df['hierarchi']+df['rigid'] 

df['clique'] = df['gossip']+df['cliqu']+df['nepot']+df['power']+df['drama']

df['community'] = df['commun']+df['colleagu']+df['collegi']+df['social']

df['family'] = df['famili']+df['knit']+df['friendship']+df['love']

df['team'] = df['team']+df['collabor']+df['teamwork']

df['hostile'] = df['hostil']+df['toxic']+df['stress']+df['competit']+df['fear']+df['intimid']+df['blame']+df['micromanag']+df['distrust']+df['backstab']

#keep the columns I need
df = df[['orgid','year','wordCount','bureaucratic','clique','community','family','team','hostile']]

df.to_csv('~/projects/def-mcorrito/mcorrito/gd_contractors/data/' + 'org_culture_ratings' + '.csv')



    
    







