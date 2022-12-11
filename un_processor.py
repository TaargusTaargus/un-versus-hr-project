from constants import YEARS
from json import load
from sqlite3 import connect, OperationalError

## this is a class to process votes and place them into a SQLite DB
class UNVoteProcessor:

    ## constructor
    def __init__( self, filename = ":memory:" ):

        ## create our db object
        self.db = connect( filename )

        ## setting up some tables for db
        cursor = self.db.cursor()
        cursor.execute( '''DROP TABLE IF EXISTS YEAR_RESOLUTION_COUNTRY_VOTES''' )
        cursor.execute( '''
            CREATE TABLE YEAR_RESOLUTION_COUNTRY_VOTES
            (
                YEAR INT
                , RESOLUTION_ID INT
                , COUNTRY TEXT
                , VOTE TEXT
                , PRIMARY KEY( YEAR, RESOLUTION_ID, COUNTRY )
            )
        ''' )
        cursor.execute( '''DROP TABLE IF EXISTS RESOLUTIONS''' )
        cursor.execute( '''
            CREATE TABLE RESOLUTIONS
            (
                YEAR INT
                , RESOLUTION_ID INT
                , RESOLUTION_NAME TEXT
                , PRIMARY KEY (YEAR, RESOLUTION_ID)
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
            resolutions = dict( zip( data.keys(), range( len( data ) ) ) )

            ## insert each resolution into a list
            for resolution in data:
                for country in data[ resolution ]:
                    insert.append(
                        ( year \
                          , resolutions[ resolution ] \
                          , country \
                          , data[ resolution ][ country ]
                        )
                    )

            ## now we insert into the database
            cursor = self.db.cursor()
            cursor.executemany( '''
                INSERT INTO
                YEAR_RESOLUTION_COUNTRY_VOTES ( YEAR, RESOLUTION_ID, COUNTRY, VOTE )
                VALUES ( ?, ?, ?, ? )
            ''', insert )
            cursor.executemany( '''
                INSERT INTO
                RESOLUTIONS ( YEAR, RESOLUTION_NAME, RESOLUTION_ID )
                VALUES ( ?, ?, ? )
            ''', [ ( year, el1, el2 ) for el1, el2 in resolutions.items() ]  )
            self.db.commit()

    
## first we collect our datasets by country by year
un_vote_processor = UNVoteProcessor( 'processed/model.db' )

## go through our vote collection
for year in YEARS:
    un_vote_processor.load_year( f'raw/un/{year}_unvotes.json', year )


## we open a connection to our database
cursor = un_vote_processor.db.cursor()

## this complicated little query is a way for us to classify our countries
## the logic here is actually pretty simple (despite looking complicated)
## we are calculating the overlapping voting percentages across all resoutions between a country and the US
query = '''
    WITH cte_USVotes AS
    (
        SELECT
            YEAR
            , RESOLUTION_ID
            , VOTE
        FROM YEAR_RESOLUTION_COUNTRY_VOTES
        WHERE COUNTRY = 'UNITED STATES'
    )
    , cte_TotalVotes AS
    (
        SELECT
            v.YEAR
            , v.COUNTRY
            , COUNT( v.COUNTRY ) AS N_VOTES
        FROM YEAR_RESOLUTION_COUNTRY_VOTES v
        INNER JOIN cte_USVotes us
            ON us.RESOLUTION_ID = v.RESOLUTION_ID
            AND us.YEAR = v.YEAR
        GROUP BY v.YEAR
            , v.COUNTRY
        HAVING COUNT( v.COUNTRY ) > 10
    )
    , cte_Alignment AS
    (
        SELECT
            r.YEAR
            , r.COUNTRY
            , COUNT( r.VOTE ) AS ALIGNMENT
        FROM YEAR_RESOLUTION_COUNTRY_VOTES r
        INNER JOIN cte_USVotes c
            ON c.YEAR = r.YEAR
            AND c.RESOLUTION_ID = r.RESOLUTION_ID
            AND c.VOTE = r.VOTE
        GROUP BY r.YEAR
            , r.COUNTRY
    )
    SELECT
        a.COUNTRY
        , SUM( ALIGNMENT ) * 1.0 / SUM( N_VOTES ) AS VOTING_PERCENTAGE
    FROM cte_Alignment a
    INNER JOIN cte_TotalVotes v
        ON a.YEAR = v.YEAR
        AND a.COUNTRY = v.COUNTRY
    WHERE a.COUNTRY <> 'UNITED STATES'
    GROUP BY a.COUNTRY
    ORDER BY SUM( ALIGNMENT ) * 1.0 / SUM( N_VOTES ) DESC
'''

## we now loop over our results and assign classes based on what we see
results = cursor.execute( query ).fetchall()
classes = []

## this is dumb code, but just a simple way to get classes based on insight from another analysis
for i in range( len( results ) ):

    if i <= 60:
        classes.append( ( results[ i ][ 0 ], 'ALLY' ) )
    else:
        classes.append( ( results[ i ][ 0 ], 'OPPOSED' ) )

## setting our classes
cursor = un_vote_processor.db.cursor()
cursor.execute( '''DROP TABLE IF EXISTS COUNTRY_STATUS''' )
cursor.execute( '''
    CREATE TABLE COUNTRY_STATUS
    (
        COUNTRY TEXT
        , STATUS TEXT
    )
''' )
un_vote_processor.db.commit()

cursor = un_vote_processor.db.cursor()

cursor.executemany( '''
    INSERT INTO
    COUNTRY_STATUS ( COUNTRY, STATUS )
    VALUES ( ?, ? )
''', classes )

un_vote_processor.db.commit()

"""
## quick interpreter
while True:

    try:
        sql = input( "Query to Execute:\n" )
        cursor = un_vote_processor.db.cursor()
        results = cursor.execute( sql ).fetchall()[ :50 ]
        print( "\n".join( [ str( e ) for e in results ] ) )
    except OperationalError as e:
        print( e )
    except:
        break
"""

