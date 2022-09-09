
from asyncio.windows_events import NULL
import os
from warnings import catch_warnings
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

#TODO : découper en plusieurs classes le gros paquet
class spotify_analyse:

    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    df = ""
    # todo : factoriser les multiples méthodes de connexion bdd
    __POSTGRES_ADDRESS = os.environ.get('POSTGRES_ADDRESS', 'localhost')
    __POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
    __POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME', 'postgres')
    __POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'laposte')
    __POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'spotify')
    DB_CONNECTION_COMMAND = os.environ.get('DB_CONNECTION_COMMAND', 'postgresql+psycopg2://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=__POSTGRES_USERNAME, password=__POSTGRES_PASSWORD,ipaddress=__POSTGRES_ADDRESS, port=__POSTGRES_PORT, dbname=__POSTGRES_DBNAME))
    ENGINE = create_engine(DB_CONNECTION_COMMAND)
    POSTGRES_STR = os.environ.get('POSTGRES_STR', ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=__POSTGRES_USERNAME, password=__POSTGRES_PASSWORD,ipaddress=__POSTGRES_ADDRESS, port=__POSTGRES_PORT, dbname=__POSTGRES_DBNAME)))
    df_playlist_artist_inout = pd.DataFrame()
    __sp = ''
    SPOTIFY_CLIENT_ID= os.environ.get('SPOTIFY_CLIENT_ID', '--TO-FILL-WITH-YOUR-ID--')
    SPOTIFY_CLIENT_SECRET= os.environ.get('SPOTIFY_CLIENT_SECRET', '--TO-FILL-WITH-YOUR-ID--')

    def __init__(self, playlist_ids):
        self.playlist_ids = playlist_ids

    def store_playlist_artist_inout_into_db(self, dataframe):
        return_value = 0

        POSTGRES_STR = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'\
            .format(username=self.__POSTGRES_USERNAME,\
                    password=self.__POSTGRES_PASSWORD,\
                    ipaddress=self.__POSTGRES_ADDRESS,\
                    port=self.__POSTGRES_PORT,\
                    dbname=self.__POSTGRES_DBNAME))

        for index, row in dataframe.iterrows():
            try:
                conn = psycopg2.connect(POSTGRES_STR)
                cursor = conn.cursor()
                # syntaxe pourrie car je n'ai pas réussi à faire marcher les syntaxes classiques ci-dessous
                # TODO : reeessayer la syntaxe classique
                # sql = """INSERT INTO villes(idVille,codePostal,ville,location) VALUES (%s,%s,%s,%s);"""
                # cur.execute(sql, (idVille,codePostal,ville,location),)
                sql_command = "INSERT INTO playlist_artist_inout (playlist_id, playlist_name, artist_id, artist_name, date, status) VALUES('" + str(row['playlist_id']) + "','" + str(row['playlist_name']) + "','" +  str(row['artist_id']) + "','" + str(row['artist_name']) + "','"  + str(row['date']) + "','" +  str(row['status'])+ "')"
                # print(sqlCommand)
                cursor.execute(sql_command)
                conn.commit()
                cursor.close()

            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                return_value = 3
            finally:
                if conn is not None:
                    conn.close()
        return return_value

    def create_database(self):
        
        return_value = 0

        try:
            #establishing the connection
            conn = psycopg2.connect(database="postgres", user=self.__POSTGRES_USERNAME, password=self.__POSTGRES_PASSWORD, host=self.__POSTGRES_ADDRESS, port= self.__POSTGRES_PORT)
            conn.autocommit = True

            #Creating a cursor object using the cursor() method
            cursor = conn.cursor()

            #Preparing query to create a database
            sql = '''CREATE database spotify''';

            #Creating a database
            cursor.execute(sql)
            print("Database created successfully........")

            #Closing the connection
            conn.close()

        except (Exception, psycopg2.DatabaseError) :
            return_value = 3

        return return_value

    def create_tables(self):
        
        """ create tables in the PostgreSQL database"""
        commands = (
            """
            CREATE TABLE IF NOT EXISTS public.playlist_artist_inout
            (
                id bigserial  NOT NULL,
                playlist_id text COLLATE pg_catalog."default",
                playlist_name text COLLATE pg_catalog."default",
                artist_id text COLLATE pg_catalog."default",
                artist_name text COLLATE pg_catalog."default",
                date timestamp with time zone,
                status text COLLATE pg_catalog."default",
                CONSTRAINT playlist_artist_inout_pkey PRIMARY KEY (id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.artist_popularity
            (
                id bigserial  NOT NULL,
                artist_id text COLLATE pg_catalog."default",
                artist_name text COLLATE pg_catalog."default",
                date timestamp with time zone,
                popularity bigint,
                CONSTRAINT artist_popularity_pkey PRIMARY KEY (id)
            )
            """,
            """
            ALTER TABLE IF EXISTS public.playlist_artist_inout OWNER to postgres;
            """,
            """
            ALTER TABLE IF EXISTS public.artist_popularity OWNER to postgres;
            """)
        conn = None
        try:
            # connect to the PostgreSQL server
            conn = psycopg2.connect(self.__POSTGRES_STR)
            cur = conn.cursor()
            # create table one by one
            for command in commands:
                cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return_value = 3
        finally:
            if conn is not None:
                conn.close()
        return return_value

    def retrieve_artist_popularity_from_artist_id(self, artist_id):

        artist_popularity = self.__sp.artist(artist_id)
        return artist_popularity['popularity']

    def get_datas_from_playlists(self):

        return_value = 0

        self.__sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=self.SPOTIFY_CLIENT_ID, client_secret=self.SPOTIFY_CLIENT_SECRET))
        
        # TODO ne gere pas les playlists avec un numero erroné
        list_item = []
        for id in range(len(self.playlist_ids)):
            try:
                playlist = self.__sp.playlist(self.playlist_ids[id])
                playlist_id = playlist['uri'].split(':')[2]
                playlist_tracks = self.__sp.playlist_tracks(playlist_id)
                track_items = playlist_tracks['items']
                # gestion de la pagination de l'api spotify
                while playlist_tracks['next']:
                    playlist_tracks = sp.next(playlist_tracks)
                    track_items.extend(playlist_tracks['items'])
                for track in track_items:
                    occurrence = {}
                    if track['track'] is not None :
                        occurrence['playlist_id'] = playlist_id
                        occurrence['playlist_name'] = playlist['name']
                        occurrence['album_name'] = track["track"]["album"]["name"]
                        for artist in track['track']['artists']:
                            occurrence["artist_id"] = artist["id"]
                            occurrence["artist_name"] = artist['name']
                            occurrence['artist_popularity'] = self.retrieve_artist_popularity_from_artist_id(artist["id"])
                            list_item.append(occurrence)
            except ValueError:
                print(ValueError)

        self.df = pd.DataFrame(list_item)
        # TODO add drop_duplicates() to reduce data amount

        return return_value

    def retrieve_last_date_playlist_artist_inout_from_playlist_id(self, playlist_id):
        
        # Connect to PostgreSQL server
        db_connection = create_engine(self.DB_CONNECTION_COMMAND).connect()

        # Read data from PostgreSQL database table and load into a DataFrame instance
        df = pd.read_sql("select max(date) from playlist_artist_inout where playlist_id = %(playlist_id)s", db_connection, params={'playlist_id':playlist_id}, parse_dates={'date'})

        # Close the database connection
        db_connection.close()

        last_date = df.iat[0,0]

        if last_date is None:
                last_date=""
        
        return last_date


    def retrieve_last_playlist_artist_inout_from_playlist_id(self, playlist_id):
        
        # Guillaume est parti d'une requete avec jointure plutôt que 2 requtes (date max puis datas)
        # ca fait effectivement moisn de requêtes
        # SELECT t1.* FROM playlist_artist_inout t1 JOIN (\
        #              SELECT playlist_id, artist_id, MAX(date) as MAXDATE\
        #              FROM playlist_artist_inout\
        #              WHERE date < '" + str(date_du_jour) + "'"\
        #              "GROUP BY playlist_id, artist_id\
        #          ) t2\
        #          ON t1.playlist_id = t2.playlist_id\
        #          AND t1.artist_id   = t2.artist_id\
        #          AND t1.date        = t2.MAXDATE"
        last_date = self.retrieve_last_date_playlist_artist_inout_from_playlist_id(playlist_id)
        # Connect to PostgreSQL server
        db_connection = create_engine(self.DB_CONNECTION_COMMAND).connect()

        if last_date != "":
            # Read data from PostgreSQL database table and load into a DataFrame instance
            df = pd.read_sql('select * from playlist_artist_inout where playlist_id = %(playlist_id)s AND date=%(last_date)s', db_connection, params={'playlist_id':playlist_id, 'last_date':last_date})
        else:
            # Read data from PostgreSQL database table and load into a DataFrame instance
            df = pd.read_sql('select * from playlist_artist_inout where playlist_id = %(playlist_id)s', db_connection, params={'playlist_id':playlist_id})

            df['date'] = pd.to_datetime(df['date'])

        db_connection.close()
        return df

    def retrieve_last_playlist_artist_inout_from_playlists(self):
        df = pd.DataFrame()
        for playlist_id in self.playlist_ids:
            df2 = self.retrieve_last_playlist_artist_inout_from_playlist_id(playlist_id)
            df = pd.concat([df, df2])
             # TODO add drop_duplicates() to reduce data amount
        return df

    def __create_dataset_playlist_artist_inout(self):
        if isinstance(self.df, str):
            self.get_datas_from_playlists()
        df_playlist_artist_inout = self.df[['artist_id','artist_name','playlist_id','playlist_name']].drop_duplicates()
        df_playlist_artist_inout['date']=self.current_datetime
        df_playlist_artist_inout['status']='stay'
        return df_playlist_artist_inout

    def identify_artist_to_be_out(self, df_playlist_artist_inout_old, df_playlist_artist_inout):
        # recherche des anciens artistes dans les playlists
        df_artist_to_be_out = pd.merge(df_playlist_artist_inout_old, df_playlist_artist_inout, how='left', on=['artist_id','playlist_id'], suffixes=('', '_new'), indicator=True).query('_merge == "left_only"').drop('_merge',axis=1)
        df_artist_to_be_out.drop(df_artist_to_be_out.filter(regex='_new$').columns, axis=1, inplace=True)
        df_artist_to_be_out = df_artist_to_be_out[['artist_id','artist_name','playlist_id','playlist_name','status']]
        df_artist_to_be_out = df_artist_to_be_out[df_artist_to_be_out.status != 'out']
        df_artist_to_be_out['date']=self.current_datetime
        df_artist_to_be_out['date'] = pd.to_datetime(df_artist_to_be_out['date'])
        df_artist_to_be_out['status']='out'
        
        return df_artist_to_be_out

    def identify_artist_to_be_in(self, df_playlist_artist_inout_old, df_playlist_artist_inout):
        
        # recherche des nouveaux artistes dans les playlists
        df_artist_to_be_in = pd.merge(df_playlist_artist_inout_old,df_playlist_artist_inout, how='right', on=['artist_id','playlist_id'], suffixes=('_old', ''), indicator=True).query('_merge == "right_only"').drop('_merge',axis=1)
        df_artist_to_be_in.drop(df_artist_to_be_in.filter(regex='_old$').columns, axis=1, inplace=True)
        df_artist_to_be_in = df_artist_to_be_in[['artist_id','artist_name','playlist_id','playlist_name']]
        df_artist_to_be_in['date']=self.current_datetime
        df_artist_to_be_in['date'] = pd.to_datetime(df_artist_to_be_in['date'])
        df_artist_to_be_in['status']='in'
        
        return df_artist_to_be_in

    def identify_artist_to_stay(self, df_playlist_artist_inout_old, df_playlist_artist_inout):
        
        # recherche des artistes déjà présents dans les playlists
        df_artist_to_stay= pd.merge(df_playlist_artist_inout_old,df_playlist_artist_inout, how='inner', on=['artist_id','playlist_id'], suffixes=('_old', ''), indicator=True)
        df_artist_to_stay = df_artist_to_stay[['artist_id','artist_name','playlist_id','playlist_name']]
        df_artist_to_stay['date']=self.current_datetime
        df_artist_to_stay['date'] = pd.to_datetime(df_artist_to_stay['date'])
        df_artist_to_stay['status']='stay'
        
        return df_artist_to_stay
    
    def retrieve_artist_in_out(self):
        df_playlist_artist_inout = self.__create_dataset_playlist_artist_inout()
        df_playlist_artist_inout_old = self.retrieve_last_playlist_artist_inout_from_playlists()
        df_artist_to_be_out = self.identify_artist_to_be_out(df_playlist_artist_inout_old, df_playlist_artist_inout)
        df_artist_to_be_in = self.identify_artist_to_be_in(df_playlist_artist_inout_old, df_playlist_artist_inout)
        df_artist_to_stay = self.identify_artist_to_stay(df_playlist_artist_inout_old, df_playlist_artist_inout)
        df_artist_to_insert = pd.concat([df_artist_to_be_in, df_artist_to_be_out, df_artist_to_stay], ignore_index=True)
        return df_artist_to_insert

    def retrieve_artist_popularity(self):

        if isinstance(self.df, str):
            self.get_datas_from_playlists()

        # Création du datasert artist_popularity
        df_artist_popularity = self.df[['artist_id','artist_name', 'artist_popularity']].drop_duplicates()
        df_artist_popularity['date']=self.current_datetime
        df_artist_popularity = df_artist_popularity.rename(columns={"artist_popularity": "popularity"})

        return df_artist_popularity
    
    def store_artist_popularity(self, df_artist_popularity):

        return_value = 0
        try:
            # Enregistrement des données
            df_artist_popularity.to_sql('artist_popularity', self.ENGINE, if_exists='append', index=False)
        except (Exception) as error:
            print(error)
            return_value = 3

        return return_value
    
    def get_artist_popularity_by_date(self, from_date, to_date):
    
        return_value = NULL
        try:
            return_value = pd.read_sql("SELECT * FROM playlist_artist_inout where date >= '"+ from_date + "' AND date <= '"+ to_date + "' AND status != 'stay'", self.ENGINE)
        
        except (Exception) as error:
            print(error)

        return return_value
    
    def test_easy(self):
        return 1

# configuration
playlist_ids=['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz']
sa = spotify_analyse(playlist_ids)

# récupération des populatrités
df_artist_popularity = sa.retrieve_artist_popularity()
# sauvegarde des populatrités
sa.store_artist_popularity(df_artist_popularity)

# récupération des artistes entree/sortie
df_artist_inout = sa.retrieve_artist_in_out()
# sauvegarde des artistes entree/sortie
sa.store_playlist_artist_inout_into_db(df_artist_inout)

