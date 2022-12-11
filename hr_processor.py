from collections import defaultdict
from constants import YEARS
from json import dump, dumps, load
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from re import sub
from sqlite3 import connect, OperationalError
from utilities import IRDictionary

## REGEX TO REMOVE ANY HTML FORMATTING
HTML_TAG = '</?\w+?>'

## this is a class to process votes and place them into a SQLite DB
class HRReportProcessor:

    ## constructor
    def __init__( self, filename = ":memory:" ):

        ## create our db object
        self.db = connect( filename )
        self.docid = 0

        ## setting up some tables for db
        cursor = self.db.cursor()
        cursor.execute( '''DROP TABLE IF EXISTS YEAR_COUNTRY_SECTION_RAWTEXT''' )
        cursor.execute( '''
            CREATE TABLE YEAR_COUNTRY_SECTION_RAWTEXT
            (
                DOCID INT PRIMARY KEY
                , YEAR INT
                , COUNTRY INT
                , SECTION TEXT
                , RAWTEXT TEXT
                , FORMATTED TEXT
            )
        ''' )
        self.db.commit()
        

    ## fuction to load votes file
    ## args:
    ## - file: name of votes file to load
    ## - year: the year of the data being processed
    def load_year( self, filename, year ):

        ## open the file and store the votes
        with open( filename, 'r' ) as fh:

            ## set up some variables for processing
            data = load( fh )
            insert = []
            tag_map = defaultdict( lambda : wordnet.NOUN )
            tag_map[ 'J' ] = wordnet.ADJ
            tag_map[ 'V' ] = wordnet.VERB
            tag_map[ 'R' ] = wordnet.ADV

            ## insert each resolution into a list
            for country in data:
                for section in data[ country ]:

                    print( f'Processing document {self.docid}...' )
                    
                    raw = data[ country ][ section ]

                    ## we perform some text processing
                    ## processing is based on guide posted here: https://medium.com/@bedigunjit/simple-guide-to-text-classification-nlp-using-svm-and-naive-bayes-with-python-421db3a72d34
                    formatted = sub( HTML_TAG, "", raw )
                    formatted = word_tokenize( formatted )

                    words = []
                    word_lemmatizer = WordNetLemmatizer()

                    for word, tag in pos_tag( formatted ):

                        if word not in stopwords.words( 'english' ) and word.isalpha():
                            words.append( word_lemmatizer.lemmatize( word, tag_map[ tag[ 0 ] ] ) )

                    final_formatted = " ".join( words )
                    
                    ## now we insert into our database
                    insert.append(
                        (
                            self.docid
                          , year 
                          , country 
                          , section 
                          , raw 
                          , final_formatted
                        )
                    )
                    self.docid = self.docid + 1

            ## now we insert into the database
            cursor = self.db.cursor()
            cursor.executemany( '''
                INSERT INTO
                YEAR_COUNTRY_SECTION_RAWTEXT ( DOCID, YEAR, COUNTRY, SECTION, RAWTEXT, FORMATTED )
                VALUES ( ?, ?, ?, ?, ?, ? )
            ''', insert )
            self.db.commit()



## initalize some accounting variables
hr_report_processor = HRReportProcessor( 'processed/model.db' )

## go through our vote collection
for year in YEARS:
    print( f'Processing file for {year}...' )
    hr_report_processor.load_year( f'raw/hr/{year}_hrreports.json', year )


"""
print( f'Processing files...' )
corpus = []
dmap = {}
docid = 0

## we proces our hr reports into an index file
for year in YEARS:

    contents = {}
    
    with open( f'raw/hr/{year}_hrreports.json', 'r' ) as fh:
        print( f'Processing file {year}_hrreports.json ...' )
        contents = load( fh )

    for country in contents:

        for section in contents[ country ]:
        
            dmap[ str( docid ) ] = {
                'country': country
                , 'year': year
            }
            corpus.append( contents[ country ][ section ] )
            docid = docid + 1

print( f'Creating index and dictionary files...' )

## setting up some variables
dictionary = IRDictionary()

DICTIONARY_FILENAME = 'processed/hr/dictionary.json'
INDEX_FILENAME = 'processed/hr/index.bin'

dictionary.read_collection( corpus )
dictionary.attach_index( INDEX_FILENAME )

dictionary.write_index( INDEX_FILENAME )
dictionary.write_dictionary( DICTIONARY_FILENAME )

with open( f'processed/hr/document_map.json', 'w' ) as fh:
    print( 'Writing document map...' )
    fh.write( dumps( dmap, indent = 4 ) )
"""
