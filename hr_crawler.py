from bs4 import BeautifulSoup
from constants import DEBUG, COUNTRIES, YEARS, HR_CONFIG
from json import dumps
from re import DOTALL, findall
from urllib.request import urlopen
from utilities import download, unpack

LINK_REGEX = '"(https://www.state.gov/reports/{year}-country-reports-on-human-rights-practices/.*?)"'
SECTION_REGEX = '<section class=".*?content">(.*?)</section>'
SUBTITLE_REGEX = '<h3.*?>(.*?)</h3>'
TEXT_REGEX = '<p>(.*?)</p>'
TITLE_REGEX = '<h2.*?>(.*?)</h2>'

## we go through each year
for year in ['2021']: #YEARS:

    ## unpack our base url based on uear
    url = unpack( HR_CONFIG[ 'baseURL' ], dict( zip( HR_CONFIG[ "q" ], [ year ] ) ) )

    if DEBUG:
        print( f'... Fetching data from URL: {url}' )

    ## we fetch data from the URL
    contents = {}
    data = download( url )
    soup = BeautifulSoup( data, 'html.parser' )

    
    
    ## we now find all of the unique URLs for countries
    url_re = unpack( LINK_REGEX, dict( zip( HR_CONFIG[ "q" ], [ year ] ) ) )
    urls = [ findall( url_re, str( el ), flags = DOTALL ) for el in soup.find_all( 'a' ) ]
    urls = set( [ el[ 0 ] for el in urls if el ] )

    ## now we loop over those urls
    for url in urls:

        ## we fetch data from the URL
        article = {}
        country = url.split( '/' )[ -2 ].upper()
        data = download( url )
        soup = BeautifulSoup( data, 'html.parser' )

        if DEBUG:
            print( f'... Processing {country} in the year {year}' )

        ## we loop through HTML sections for text
        for section in soup.find_all( 'section' ):

            ## convert to string please
            section = str( section )

            ## we look only for sections with relevant text
            tsection = findall( SECTION_REGEX, section, flags = DOTALL )

            ## if not a match... we continue!
            if not len( tsection ):
                continue

            title = findall( TITLE_REGEX, section, flags = DOTALL )

            if title:
                title = title[ 0 ]

            subtitles = list( findall( SUBTITLE_REGEX, section, flags = DOTALL ) )

            ## we want to avoid duplicates
            if len( subtitles ) != 1:
                continue

            subtitle = subtitles[ 0 ]
            text = " ".join( findall( TEXT_REGEX, section, flags = DOTALL ) )
            
            article[ subtitle ] = text

        contents[ country ] = article

    
    ## open file to write to
    with open( f'raw/hr/{year}_hrreports.json', 'w' ) as fh:
        print( f'Writing to file...' )
        fh.write( dumps( contents, indent = 4 ) )
