import csv, lda
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
  

def ldaModel(minCount,num_topics,field,a,b):
    #load phrase doc-term matrix
    df = pd.read_csv('/ifs/gsb/mcorrito/gd_contractors/' + 'top_unigrams_' + field + '_'+ str(minCount) + '.csv',sep=',',nrows=1000)
    
    #calculate review word count and drop if <5 words
    df2 = df.drop(df.columns[[0]],axis=1)
    wordCount = np.sum(df2,axis=1)

    print(wordCount)
    
    df['word_count'] = wordCount
    df = df[df['word_count'] >= 5]
    df.reset_index()
    count = df['word_count']

    print(count)

    df = df.drop(['word_count'], axis=1)
    
    #remove ids
    ids = df.drop(df.columns[1:len(df.columns)],axis=1)
    df = df.drop(df.columns[[0]],axis=1)

    #get words
    words = list(df.columns.values)

    #convert to matrix
    mat = df.as_matrix()
    
    #lda 
    model = lda.LDA(n_topics=num_topics, n_iter=700, random_state=1,alpha=a,eta=b)
    model.fit(mat)

    topic_word = model.topic_word_
    doc_topic = model.doc_topic_
    n_top_words = 25

    #plot and save convergence graph
    plt.plot(model.loglikelihoods_[5:])
    plt.savefig('/ifs/gsb/mcorrito/gd_contractors/output/' +
    'lda_convergence_' + field + '_' + str(minCount) + '_' +
    str(num_topics) + '_' + str(a) + '_' + str(b) + '.png')
    plt.close()


    ###save model components for use and evaluation
    ##save top words for each topic to csv file
    with open('/ifs/gsb/mcorrito/gd_contractors/output/' + field + '_' +
              str(minCount) + '_' + str(num_topics) + '_' + 'lda_words' +
              '_' + str(a) + '_' + str(b),'w') as csvfile:
        writer = csv.writer(csvfile,delimiter=',',lineterminator='\n')
        
        for i, topic_dist in enumerate(topic_word):
            topic_words = np.array(words)[np.argsort(topic_dist)][:-n_top_words:-1]
            writer.writerow(['Topic {}: {}'.format(i, ' '.join(topic_words))])
    csvfile.close()

    #append ids onto the topic proportions and save as csv 
    df = pd.DataFrame(doc_topic)

    print(df.shape)
    print(ids.shape)
    print(count.shape)

    print(count)
        
    df['reviewid'] = ids
    df['word_count2'] = count

    #print(df.head(20))
        
    df.to_csv('/ifs/gsb/mcorrito/gd_contractors/output/' + 'lda_final_' + field +
    '_' + str(minCount) + '_' + str(num_topics) + '_' + str(a) + '_' + str(b) + '.csv')

    

ldaModel(25,100,'pro', 0.1,0.1)
#ldaModel(25,100,'con', 0.1,0.1)


    
    







