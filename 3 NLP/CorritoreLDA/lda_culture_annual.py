import csv, lda
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
  

def ldaModel(numWords,num_topics):
    #load phrase doc-term matrix
    df = pd.read_csv('/project/def-mcorrito/mcorrito/duality/' + 'top_unigrams_phrase_' + str(numWords) + '_pruned.csv',sep=',',dtype='int32')

    #remove ids
    df = df.drop(df.columns[[0,1,2,3]],axis=1)

    print(df.shape)
    
    #get words
    words = list(df.columns.values)

    #convert to matrix
    mat = df.as_matrix()
    
    #lda (use 350 iterations)
    model = lda.LDA(n_topics=num_topics, n_iter=700, random_state=1)
    model.fit(mat)
    topic_word = model.topic_word_
    doc_topic = model.doc_topic_
    n_top_words = 100

    #plot and save convergence graph
    plt.plot(model.loglikelihoods_[5:])
    plt.savefig('/project/def-mcorrito/mcorrito/gd_contractors/output/' + 'lda_convergence_' + str(numWords) + '_' + str(num_topics) + '.png')
    plt.close()


    ###save model components for use and evaluation
    ##save top words for each topic to csv file
    with open('/project/def-mcorrito/mcorrito/gd_contractors/output/' + str(numWords) + '_' + str(num_topics) + '_' + 'lda_words','w') as csvfile:
        writer = csv.writer(csvfile,delimiter=',',lineterminator='\n')
        
        for i, topic_dist in enumerate(topic_word):
            topic_words = np.array(words)[np.argsort(topic_dist)][:-n_top_words:-1]
            writer.writerow(['Topic {}: {}'.format(i, ' '.join(topic_words))])
    csvfile.close()

    print("apply model to working sample")    

    
    #apply model to working sample#######################################################################
    #free up memory
    del df
    df = pd.read_csv('/project/def-mcorrito/mcorrito/gd_contractors/data/' + 'top_unigrams_annual_' + str(numWords) + '.csv',sep=',',dtype='int32')

    #remove ids and save for later concat
    ids = df.drop(df.columns[3:len(df.columns)],axis=1)
    df = df.drop(df.columns[[0,1,2]],axis=1)

    print(df.shape)

    df = df.dropna()

    print(df.shape)

    #calculate review word count and add to ids
    wordCount = np.sum(df,axis=1)

    #convert to matrix
    mat = df.as_matrix()

    mat = mat.astype(np.int64)
    
    print(mat.dtype)
    
    #apply model
    print("BEGIN")
    modelTest = model.transform(mat)
    print("DONE")
    
    #append ids onto the topic proportions and save as csv 
    df = pd.DataFrame(modelTest)
    df = pd.concat([ids,wordCount,df],axis=1,ignore_index=True)

    df.to_csv('/project/def-mcorrito/mcorrito/gd_contractors/output/' + 'lda_final_' + str(numWords) + '_' + str(num_topics) + '_annual.csv')

    print("MODEL DONE")
    
ldaModel(4000,500)
#ldaModel(4000,100)

    
    







