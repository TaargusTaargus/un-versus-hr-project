from urllib.request import urlopen
from json import dump, dumps, load
from os import stat
from re import DOTALL, findall, match, sub
from time import time
from nltk.corpus import stopwords

## some constants
DEBUG = 1
INDEX_ENTRY_SIZE = 4

## some porter's constants
M0 = "[^aeiou]*[aeiou]*$"
M1 = "[^aeiou]?[aeiou]+[^aeiou]+[aeiou]?$"


## class to process tokens to terms
class IRAdvancedTermProcessor:

    ## function performs first step of Porter's algorithm, description of implementation taken from: http://facweb.cs.depaul.edu/mobasher/classes/csc575/papers/porter-algorithm.html
    ## arguments:
    ##      - token: token to be stemmed
    def porters( self, token ):

        m = 0
        if match( M0, token ):
            #print( f'M0: {token}' )
            m = 0
        elif match( M1, token ):
            #print( f'M1: {token}' )
            m = 1
        else:
            #print( f'M2+: {token}' )
            m = 2

        
        ## step 1a
        if match( "(\w+?)sses$", token ):
            token = sub( r"(\w+?)sses$", "\g<1>ss", token )

        elif match( "(\w+?)ies$", token ):
            token = sub( r"(\w+?)ies$", "\g<1>i", token )

        else:
            token = sub( r"(\w+?[^aiou])s$", "\g<1>", token )

        ## step 1b
        if m > 0:
            token = sub( r"(\w+?)eed$", "\g<1>ee", token )

        elif match( "(.*?[aeiou].*?)ed", token ):
            
            token = sub( "(.*?[aeiou].*?)ed", "\g<1>", token )

            if match( "(.*?)at", token ):
                token = sub( "(.*?)at", "\g<1>ate", token )

            elif match( "(.*?)bl", token ):  
                token = sub( "(.*?)bl", "\g<1>ble", token )

            elif match( "(.*?)iz", token ):  
                token = sub( "(.*?)iz", "\g<1>ize", token )

            ## double same consonant ending check
            elif token[ -1 ] == token[ -2 ] and token[ -1 ] not in ( 'l', 's', 'z' ):
                token = token[ : -1 ]

            elif m == 1 and match( '[^aeiou][aeiou][^aeiou]', token ):
                token = token + 'e'

        elif match( "(.*?[aeiou].*?)ing", token ):
            
            token = sub( "(.*?[aeiou].*?)ing", "\g<1>", token )

            if match( "(.*?)at", token ):
                token = sub( "(.*?)at", "\g<1>ate", token )

            elif match( "(.*?)bl", token ):  
                token = sub( "(.*?)bl", "\g<1>ble", token )

            elif match( "(.*?)iz", token ):  
                token = sub( "(.*?)iz", "\g<1>ize", token )

            ## double same consonant ending check
            elif token[ -1 ] == token[ -2 ] and token[ -1 ] not in ( 'l', 's', 'z' ):
                token = token[ : -1 ]

            elif m == 1 and match( '[^aeiou][aeiou][^aeiou]', token ):
                token = token + 'e'


        return token


    ## function to transforms a document to tokens and then normalize to terms, specifically:
    ##      - split on spaces
    ##      - remove special characters
    ##      - convert all to lower case
    ##      - remove numbers
    ##      - remove newlines
    ##      - first step of Porter's for plural and conjugated verbs
    ## arguments:
    ##      - document: the document to be tokenized
    def tokenize_and_normalize( self, document ):

        ## first we strip out any newlines or whitespace
        document = document.strip()

        ## we perform case-folding
        document = document.lower()

        ## we replace hyphens with spaces
        document = sub( r"-", " ", document )

        ## now we remove special characters
        document = sub( r"[^a-zA-Z0-9_ \n]", '', document )

        ## we now tokenize
        tokens = document.split( " " )

        ## lets throw my implementation of Porter's at it, why not?
        stops = set( stopwords.words( 'english' ) )
        
        for i in range( len( tokens ) ):
            
            ## stopword removal
            if tokens[ i ] in stops:
                continue
            
            tokens[ i ] = self.porters( tokens[ i ] )


        

        return len( tokens ), list( tokens )
            

## Dictionary object
class IRDictionary:

    ## constructor for Dictionary class
    def __init__( self, processor = IRAdvancedTermProcessor() ):
        self.dictionary = {
            'collection_size': 0  ## this is the total numbers of tokens processed
            , 'index_file': None  ## this is the filepath of the index file to be used
            , 'document_count': 0 ## this is the total number of documents
            , 'uniques' : 0       ## this is the total number of unique words
            , 'dictionary': {} ## this is the lexicon
            , 'weights': None
        }
        self.postings = {}
        self.processor = processor
        

    ## function to read a collection of documents from a single file
    ## arguments:
    ##      - documents: the corpus of documents to be processed
    def read_collection( self, documents ):

        ## loop until the end of time
        ## reading all file contents causes MemoryError
        ## so we read documents one at a time
        for docid in range( len( documents ) ):

            document = documents[ docid ]

            ## tokenize and normalize
            n_tokens, tokens = self.processor.tokenize_and_normalize( document )

            ## we loop over our terms
            for token in tokens:

                ## we check to see if the term is already in our postings list
                ## if it is then add this docID to the list, if not init the list with this docID
                if token in self.postings:
                    
                    if docid in self.postings[ token ]:
                        self.postings[ token ][ docid ] = self.postings[ token ][ docid ] + 1
                    else:
                        self.postings[ token ][ docid ] = 1
                        
                else:
                    self.postings[ token ] = { docid: 1 }

            ## keep track of the total number of tokens processed and documents processed
            self.dictionary[ 'collection_size' ] = self.dictionary[ 'collection_size' ] + n_tokens
            self.dictionary[ 'document_count' ] = self.dictionary[ 'document_count' ] + 1


        ## print some basic statistics
        if DEBUG > 0:
            print( f'Total Documents Processed: {int( docid ) + 1}' )
            print( f'Total Unique Words (Terms): {len(self.postings)}' )
            print( f"Total Amount of Words (Tokens): {self.dictionary[ 'collection_size' ]}" )


        ## we now assemble the dictionary from our postings list
        counter = 0
        self.dictionary[ 'dictionary' ] = {}
        self.dictionary[ 'uniques' ] = len(self.postings)
        
        for term in self.postings:
            self.dictionary[ 'dictionary' ][ term ] = {
                'id': counter
                , 'document_frequency': len( self.postings[ term ] )
                , 'term_frequency': sum( [ self.postings[ term ][ docid ] for docid in self.postings[ term ] ]  )
                , 'offset': 0
            }
            counter = counter + 1


    ## function to write our postings list to an inverted index file
    ## as currently designed this is algorithm A
    ## arguments:
    ##      - index_file: the name of the index file we want to write to
    def write_index( self, index_file ):

        offset = 0
        ret = ""

        ## open a connection to our index file
        with open( index_file, 'wb' ) as file:

            for term in sorted( self.postings.keys() ):

                self.dictionary[ 'dictionary' ][ term ][ 'offset' ] = offset

                ## we write out our postings list to our index file and then note the offset
                for docid in sorted( self.postings[ term ].keys() ): 

                        ## get our docid and frequency
                        freq = int( self.postings[ term ][ docid ] )
                        docid = int( docid )

                        ## write postings list data
                        file.write( docid.to_bytes( INDEX_ENTRY_SIZE, byteorder='big', signed=False ) )
                        file.write( freq.to_bytes( INDEX_ENTRY_SIZE, byteorder='big', signed=False ) )

                        ## note the offset, we advance it by eight bytes for each entry in the postings list
                        offset = offset + INDEX_ENTRY_SIZE * 2


    ## function to write our dictionary to a json file
    ## arguments:
    ##      - dictionary_file: the name of the dictionary file we want to write to
    def write_dictionary( self, dictionary_file ):

        with open( dictionary_file, 'w' ) as fh:
            dump( self.dictionary, fh )


    ## function to retrieve term statistics from the dictionary
    ## arguments:
    ##      - term: a term to check in this dictionary
    def print_term_statistics( self, term ):

        lterm = term.lower()
        if lterm in self.dictionary[ 'dictionary' ]:
            print( f"'{term}' Statistics: {self.dictionary[ 'dictionary' ][ lterm ]}" )
            return self.dictionary[ 'dictionary' ][ lterm ]
        else:
            print( f"'{term}' not present in dictionary." )
            return None


    ## function to attach an index file to this dictionary for postings
    ## arguments:
    ##      - index_file: path to index file, this should be a binary file
    def attach_index( self, index_file ):
        self.dictionary[ 'index_file' ] = index_file


    ## function to load dictionary from a file
    ## arguments:
    ##      - dictionary_file: path to .json dictionary file to load
    def load_dictionary( self, dictionary_file ):
        
        with open( dictionary_file, 'r' ) as fh:
            self.dictionary = load( fh )
        

    ## remove all postings
    def flush_postings( self ):
        self.postings = {}

        
    ## function to retrieve postings list from an index file
    ## arguments:
    ##      - entry: IRDictionary entry of the term to be fetched
    def retrieve_postings_from_index_from_index( self, entry ):

        postings = {}

        # if we have no entry, just return an empty list 
        if not entry:
            return postings

        # we open our index file and fetch our tuples starting at the offset in the dictionary
        with open( self.dictionary[ 'index_file' ], 'rb' ) as idx:

            # advance read to the offset place
            idx.read( entry[ 'offset' ] )

            # get the postings, which we can calculate as being =  {doc count} * {size of posting entry} * 2
            size = entry[ 'document_frequency' ] * INDEX_ENTRY_SIZE * 2
            content = idx.read( size )

            # iterate over contents
            for i in range( 0, size, INDEX_ENTRY_SIZE * 2 ):             
                docid = int.from_bytes( content[ i : i + INDEX_ENTRY_SIZE ], byteorder = 'big', signed = False )
                freq = int.from_bytes( content[ i + INDEX_ENTRY_SIZE : i + INDEX_ENTRY_SIZE * 2 ], byteorder = 'big', signed = False )
                postings[ str( docid ) ] = freq

        return postings


            
## scrapes a website for documents
## args:
##      - url: url to be scraped
def download( url ):

    ## get the website address
    contents = ""
    with urlopen( url ) as handle:
        contents = handle.read().decode( 'utf-8' )
    
    ## scrape the contents with regex
    return contents


## unpacks a query to a base url
## args:
##      - base: baseurl
##      - q: any queries
def unpack( base, q = [] ):

    ## loop through query
    for qe in q:
        base = base.replace( '{' + qe + '}', q[ qe ] )

    ## bind the baseurl and the query
    return base
