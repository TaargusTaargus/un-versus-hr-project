from bs4 import BeautifulSoup
from constants import DEBUG, COUNTRIES, YEARS, UN_CONFIG
from json import dumps, load
from re import DOTALL, findall
from utilities import download, unpack


## serach results querying info
LINK_REGEX = '<a class="moreinfo" href="/record/(.*?)">'
PAGE_SIZE = 200

## content web page info
TITLE_DIV_REGEX = '<div class="full-record-title">(.*?)</div>'
VOTE_DIV_REGEX = '\A<div class="metadata-row"><span class="title col-xs-12 col-sm-3 col-md-2">Vote[^\w][^\w]'
VOTE_SPAN_REGEX = '<span class="value col-xs-12 col-sm-9 col-md-10">(.*?)</span>'
VOTE_PATTERN_REGEX = '([YNA\s]) (.*?)<br/?>'
SECOND_VOTE_PATTERN_REGEX = ' ([YNA\s]) (.*)'


## we go through each year
for year in YEARS:

    ## this is our vote content dictionary
    content = {}
    
    ## we now loop over pages
    for i in range( 0, PAGE_SIZE * 10, PAGE_SIZE ):

        ## this will count our total results from the last page
        pages = []

        ## we unpack the URL
        url = unpack( UN_CONFIG[ 'baseURL' ], dict( zip( UN_CONFIG[ "q" ], [ str( i ), str( PAGE_SIZE ), str( year ) ] ) ) )

        if DEBUG:
            print( f'... Fetching data from URL: {url}' )

        ## we fetch data from the URL
        article = {}
        data = download( url )
        soup = BeautifulSoup( data, 'html.parser' )

        ## we loop through HTML sections for text
        for section in soup.find_all( 'a' ):

            ## convert to string please
            section = str( section )

            ## we look only for sections with relevant text
            tsection = findall( LINK_REGEX, section, flags = DOTALL )

            ## if not a match... we continue!
            if not len( tsection ):
                continue

            ## append result to the last page length
            pages.append( tsection[ 0 ] )
                

        ## loop over the found pages:
        for page in pages:

            ## get the data
            data = str( download( f'https://digitallibrary.un.org/record/{page}' ) )
            soup = BeautifulSoup( data, 'html.parser' )
            title, votes = None, None

            ## loop over the div tags until we find the voting meat
            for el in soup.find_all( 'div' ):

                ## regex pattern for title
                if findall( TITLE_DIV_REGEX, str( el ), flags = DOTALL ):
                    title = str( el )                

                ## regex pattern for votes
                if findall( VOTE_DIV_REGEX, str( el ), flags = DOTALL ):
                    votes = str( el )

            
            print( f'Results at: https://digitallibrary.un.org/record/{page}' )
            title = findall( TITLE_DIV_REGEX, title, flags = DOTALL )[ 0 ].strip()
            votes = findall( VOTE_PATTERN_REGEX, votes, flags = DOTALL )
            votes = dict( [ ( el2, el1 ) for el1, el2 in votes ] )
            content[ title ] = votes

        ## did we hit page size? if not, stop.
        if len( pages ) < PAGE_SIZE:
            print( '... Found all results for {year}' )
            break

    ## open file to write to
    with open( f'raw/un/{year}_unvotes.json', 'w' ) as fh:
        print( f'Writing to file...' )
        fh.write( dumps( content, indent = 4 ) )



## this is a quick algorithm to fix some messed up tags that are scraped from the UN website
for year in YEARS:

    ndata = {}

    ## we loop over our written files
    with open( f'raw/un/{year}_unvotes.json', 'r' ) as fh:

        data = load( fh )

        for key in data:

            ndata[ key ] = {}
            
            for el in data[ key ]:

                overwrite = None
                spl = el.split( "<br>" )
                
                if len( spl ) > 1:
                    overwrite = { spl[ 0 ] : data[ key ][ el ] }
                        
                    for e in el.split( "<br>" ):

                        votes = findall( SECOND_VOTE_PATTERN_REGEX, e, flags = DOTALL )
                                  
                        if votes:
                            for vote in votes:
                                  overwrite[ vote[ 1 ] ] = vote[ 0 ]

                    for e in overwrite:
                        ndata[ key ][ e ] = overwrite[ e ]
                        
                else:
                    ndata[ key ][ el ] = data[ key ][ el ]


    with open( f'raw/un/{year}_unvotes.json', 'w' ) as fh:
        print( f'Writing to file...' )
        fh.write( dumps( ndata, indent = 4 ) )
