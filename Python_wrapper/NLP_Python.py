import spacy
import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector
from spacy.lang.nl.stop_words import STOP_WORDS
import sys


def fundaNlpAnalysisFunc():
    #create connection to the databae
    #change the credentials in the db_login file before running for the first time
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    # Select the first 100 rows in the funda table and fetch them to a list object
    executing_script = "SELECT ID, fullDescription FROM funda;"
    funda = sqlio.read_sql_query(executing_script, conn)

    nlp = spacy.load("nl")
    funda_analysis = pd.DataFrame(columns=['ID','descriptionLength', 'NOUN', 'ADJ', 'VERB', 'ADV','REL_NOUN','REL_ADJ','REL_VERB','REL_ADV','EMAILS', 'URLS', 'NUMBERS','CURRENCY', 'AVG_SENTIMENT', 'lexeme_1','lexeme_2','lexeme_3','lexeme_4','lexeme_5','lexeme_6','lexeme_7','lexeme_8','lexeme_9','lexeme_10','lexeme_dict',])

    for idx, entry in funda.iterrows():
        print(idx)
        funda_analysis = pd.DataFrame(columns=['ID','descriptionLength', 'NOUN', 'ADJ', 'VERB', 'ADV','REL_NOUN','REL_ADJ','REL_VERB','REL_ADV','EMAILS', 'URLS', 'NUMBERS','CURRENCY', 'AVG_SENTIMENT', 'lexeme_1','lexeme_2','lexeme_3','lexeme_4','lexeme_5','lexeme_6','lexeme_7','lexeme_8','lexeme_9','lexeme_10'])
        document = nlp(entry['fulldescription'])

        #calculate the different parameters
        description_length = len(entry['fulldescription'].split())
        emails = []
        urls = []
        sentiment = []
        num = []
        currency = []
        NOUNS = []
        ADVS = []
        VERBS = []
        ADJS = []

        # filtering stop words
        for word in document:
            if word.is_stop==False | word.is_punct == False | word.is_space == False:
                sentiment.append(word.sentiment)
            if word.like_email==True:
                emails.append(word)
            if word.like_url==True:
                emails.append(word)
            if word.like_num==True:
                num.append(word)
            if word.is_currency==True:
                currency.append(word)
            if word.pos_=='NOUN':
                NOUNS.append(word)
            if word.pos_=='ADJ':
                ADJS.append(word)
            if word.pos_=='VERB':
                VERBS.append(word)
            if word.pos_=='ADV':
                ADVS.append(word)

        REL_NOUN = len(NOUNS)/description_length 
        REL_ADJ = len(ADJS)/description_length
        REL_VERB = len(VERBS)/description_length
        REL_ADV = len(ADVS)/description_length

        # store lexemes in DF and sort lexemes by the most used one in the description
        DF = pd.DataFrame({"lexeme": [word.lemma_ for word in document if word.is_stop==False | word.is_punct == False | word.is_space == False | word.is_currency == False | word.like_email == False | word.like_num == False]})

        #safe lexem in dict with counts to store later in table 
        lexeme = DF.groupby(['lexeme']).size().reset_index(name='counts').sort_values('counts', ascending=False)
        lexeme_1 = lexeme['lexeme'].iloc[0] if lexeme.shape[0] != 0 else 'NaN'
        lexeme_2 = lexeme['lexeme'].iloc[1] if lexeme.shape[0] > 1 else 'NaN'
        lexeme_3 = lexeme['lexeme'].iloc[2] if lexeme.shape[0] > 2 else 'NaN'
        lexeme_4 = lexeme['lexeme'].iloc[3] if lexeme.shape[0] > 3 else 'NaN'
        lexeme_5 = lexeme['lexeme'].iloc[4] if lexeme.shape[0] > 4 else 'NaN'
        lexeme_6 = lexeme['lexeme'].iloc[5] if lexeme.shape[0] > 5 else 'NaN'
        lexeme_7 = lexeme['lexeme'].iloc[6] if lexeme.shape[0] > 6 else 'NaN'
        lexeme_8 = lexeme['lexeme'].iloc[7] if lexeme.shape[0] > 7 else 'NaN'
        lexeme_9 = lexeme['lexeme'].iloc[8] if lexeme.shape[0] > 8 else 'NaN'
        lexeme_10 = lexeme['lexeme'].iloc[9] if lexeme.shape[0] > 9 else 'NaN'

        try:
            avg_sent = sum(sentiment)/len(sentiment)
        except Exception as e:
            print(e)
            avg_sent = 0

        #save lexeme info in string to store in database
        lexeme_dict = str(lexeme.to_dict('records'))
        #row = [entry['id'],description_length,len(NOUNS),len(ADJS),len(VERBS),len(ADVS),REL_NOUN,REL_ADJ,REL_VERB,REL_ADV,len(emails), len(urls),len(num),len(currency),avg_sent,lexeme_1,lexeme_2,lexeme_3,lexeme_4,lexeme_5,lexeme_6,lexeme_7,lexeme_8,lexeme_9,lexeme_10,str(lexeme_dict)]
        cols = ",".join([str(i) for i in funda_analysis.columns.tolist()])
        row_dict = {'ID':entry['id'] ,'descriptionLength':description_length, 'NOUN':len(NOUNS), 'ADJ':len(ADJS), 'VERB':len(VERBS), 'ADV':len(ADVS),'REL_NOUN':REL_NOUN,'REL_ADJ':REL_ADJ,'REL_VERB':REL_VERB,'REL_ADV':REL_ADV,'EMAILS': len(emails), 'URLS':len(urls), 'NUMBERS':len(num),'CURRENCY':len(currency), 'AVG_SENTIMENT': avg_sent, 'lexeme_1':lexeme_1,'lexeme_2':lexeme_2,'lexeme_3':lexeme_3,'lexeme_4':lexeme_4,'lexeme_5':lexeme_5,'lexeme_6':lexeme_6,'lexeme_7':lexeme_7,'lexeme_8':lexeme_8,'lexeme_9':lexeme_9,'lexeme_10':lexeme_10}
        funda_analysis = funda_analysis.append(row_dict, ignore_index=True)

        for i,row in funda_analysis.iterrows():
            try:          
                sql = "INSERT INTO funda_NLP_analysis ({}) VALUES {}".format(cols,tuple(row))
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()
    return print('The Natural language processing has been done on the fulldescription and stored in a new table in the database')

if __name__ == '__main__':
    globals()[sys.argv[1]]()
