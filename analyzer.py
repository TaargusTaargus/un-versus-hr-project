from pandas import concat, DataFrame, read_csv
from sqlite3 import connect, OperationalError

import numpy as np
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.preprocessing import LabelEncoder
from collections import defaultdict
from nltk.corpus import wordnet as wn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import model_selection, naive_bayes, svm
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from pathlib import Path  
from itertools import product


## some contants
DB_FILENAME = 'processed/model2.db'

## we open a connection to our database
db = connect( DB_FILENAME )
cursor = db.cursor()

## PULLING OUR DATA FOR RUNNING OUR MACHINE LEARNING
"""
query = '''
SELECT DISTINCT a.DOCID, LOWER( a.FORMATTED ), b.STATUS AS CLASS
FROM YEAR_COUNTRY_SECTION_RAWTEXT a
INNER JOIN COUNTRY_STATUS b
ON a.COUNTRY = b.COUNTRY
'''

## we now loop over our results and assign classes based on what we see
results = cursor.execute( query ).fetchall()

## text processing is based on guide posted here: https://medium.com/@bedigunjit/simple-guide-to-text-classification-nlp-using-svm-and-naive-bayes-with-python-421db3a72d34
df = DataFrame( results, columns = [ 'DOCID', 'TEXT', 'CLASS' ] )
print( df )

## tokenization step
df[ 'TEXT' ].dropna( inplace = True )
df[ 'TEXT' ] = [ word_tokenize( e ) for e in df[ 'TEXT' ] ]

tag_map = defaultdict( lambda : wn.NOUN )
tag_map[ 'J' ] = wn.ADJ
tag_map[ 'V' ] = wn.VERB
tag_map[ 'R' ] = wn.ADV

for index, entry in enumerate( df[ 'TEXT' ] ):

    words = []
    word_lemmatizer = WordNetLemmatizer()

    for word, tag in pos_tag( entry ):

        if word not in stopwords.words( 'english' ) and word.isalpha():
            words.append( word_lemmatizer.lemmatize( word, tag_map[ tag[ 0 ] ] ) )

    df.loc[ index, 'TEXT_FINAL' ] = " ".join( words )


filepath = Path( 'processed/tokenized.csv' )
df = df.drop( 'TEXT', axis=1 )
df.to_csv( filepath )
"""

## loading from csv file... this is updated to be able to just read from our db
df = read_csv( 'processed/tokenized.csv' )
df.dropna( inplace = True )

## do some initial transforms
train_x, test_x, train_y, test_y = model_selection.train_test_split( df[ [ 'DOCID',  'TEXT_FINAL' ] ], df[ 'CLASS' ], test_size = .3 )

encoder = LabelEncoder()
train_y = encoder.fit_transform( train_y )
test_y = encoder.fit_transform( test_y )

## here we keep track of hyperparameters
hyperparameters = {
    'max_features': [ 100000 ] # this is max features
    , 'alpha': [ 0 ]
}

## grid search!
accuracy = "accuracy,max_features,alpha\n"
combinations = product( *hyperparameters.values() )

for instance in combinations:

    args = dict( zip( hyperparameters.keys(), instance ) )

    print( f'Hyperparameters: {args} ...' )

    tfidf = TfidfVectorizer( max_features = args[ 'max_features' ] )
    tfidf.fit( df[ 'TEXT_FINAL' ] )
    train_x_tfidf = tfidf.transform( train_x[ 'TEXT_FINAL' ] )
    test_x_tfidf = tfidf.transform( test_x[ 'TEXT_FINAL' ] )

    nb = naive_bayes.MultinomialNB()
    nb.set_params( alpha = args[ 'alpha' ] )
    nb.fit( train_x_tfidf, train_y )

    predictions = nb.predict( test_x_tfidf )
    score = accuracy_score( test_y, predictions )
    accuracy = accuracy + f"{score},{args[ 'max_features' ]},{args[ 'alpha' ]}\n"
    print( f"Naive Bayes Accuracy Score -> {score * 100}" )

    recall = recall_score( test_y, predictions )
    precision = precision_score( test_y, predictions )
    f1 = f1_score( test_y, predictions )
    print( f'Recall: {recall}, Precision: {precision}, F1 Score: {f1}' )


"""
string = "word,classification\n"

for word in tfidf.vocabulary_:

    weights = DataFrame( [ word ], columns = ['TEXT'] )
    weights = tfidf.transform( weights[ 'TEXT' ] )
    prediction = nb.predict( weights )
    string = string + f'{word},{"OPPOSED" if prediction[ 0 ] else "ALLY"}\n' 

print( string )

with open( 'word_classes.csv', 'w', encoding = 'utf-8' ) as fh:
    fh.write( string )


test_y = DataFrame( test_y, columns=['CLASS'] )
predictions = DataFrame( predictions, columns=['PREDICTION'] )

dff = concat( [ predictions, test_y ], axis = 1 )

results = cursor.execute(
    '''
    SELECT DISTINCT DOCID, COUNTRY
    FROM YEAR_COUNTRY_SECTION_RAWTEXT
    '''
    ).fetchall()
lookup = dict( results )

for index, entry in enumerate( test_x[ 'DOCID' ] ):

    if entry in lookup:
        dff.loc[ index, 'DOCID' ] = entry
        dff.loc[ index, 'COUNTRY' ] = lookup[ entry ]



'''
constitution law provide independent judiciary government generally respect judicial independence impartiality constitution law provide right fair public trial independent judiciary generally enforce right defendant presume innocent authority must inform charge promptly detail trial take place without undue delay generally public judge may close defendant request minor involve defendant right present trial access legal counsel choosing government cover attorney fee defendant unable pay law require defendant find guilty reimburse government defendant right adequate time facility prepare defense avail free assistance interpreter understand speak icelandic defendant confront prosecution plaintiff witness present witness evidence discretion court prosecutor may introduce evidence police obtain illegally defendant right compel testify confess guilt defendant right appeal new court appeal landsrettur introduce january instance judgment court appeal final decision although possible refer special case final appeal supreme court report political prisoner detainee individual may seek damage cessation human right violation domestic court appeal decision involve allege violation government european convention human right echr administrative remedy also available alleged wrong
'''
        
while True:

    try:

        string = input( "Phrase or Word to Evaluate: " ).strip()
        weights = DataFrame( [ string ], columns = ['TEXT'] )
        weights = tfidf.transform( weights[ 'TEXT' ] )
        prediction = nb.predict( weights )

        if prediction[ 0 ]:
            print( "OPPOSED" )
        else:
            print( "ALLY" )

    except KeyboardInterrupt as e:
        break
"""
