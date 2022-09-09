
import os
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
import numpy as np
from pandas.testing import assert_frame_equal
from datetime import datetime

# Bidouille pour pouvoir importer les classes python du projet
import sys
from pathlib import Path
import sys
path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))
from src.spotify_data_engineer import spotify_analyse

# bidouille pour trouver les fichiers de test
path_sample_datas = os.getcwd() + '\\tests\\samples\\'

date_format= '%Y-%m-%dT%H:%M:%SZ'

def test_easy_to_validate_pytest():
   # GIVEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   # WHEN 
   actual = sa.test_easy()
   # THEN
   expected =  1

   assert actual == expected

def test_spotify_analyse_private_attribute_POSTGRES_ADDRESS():
   # GIVEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   # WHEN 
   actual = sa._spotify_analyse__POSTGRES_ADDRESS
   # THEN
   expected = 'localhost'

   assert actual == expected

def test_spotify_analyse_private_function_create_dataset_playlist_artist_inout():
   # GIVEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   sa.df = pd.read_csv(path_sample_datas + 'df_spotify_retieved_datas_claude.csv',index_col=0)
   
   # WHEN 
   actual = sa._spotify_analyse__create_dataset_playlist_artist_inout()[['artist_id','artist_name','playlist_id','playlist_name']]

   # THEN
   expected = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')[['artist_id','artist_name','playlist_id','playlist_name']]

   assert np.array_equal(expected.values, actual.values) == True

def test_identify_artist_to_be_out_test_dataframe_without_datetime():
   # GIVEN 
   df_playlist_artist_inout_old = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout_old.csv')
   df_playlist_artist_inout = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')

   # WHEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   actual = sa.identify_artist_to_be_out(df_playlist_artist_inout_old, df_playlist_artist_inout)[['artist_id','artist_name','playlist_id','playlist_name','status']]

   # THEN
   expected = pd.read_csv(path_sample_datas + 'df_artist_to_be_out.csv')[['artist_id','artist_name','playlist_id','playlist_name','status']]

   assert np.array_equal(expected.values,actual.values) == True

def test_identify_artist_to_be_out_correct_datetime_format():
   # GIVEN 
   df_playlist_artist_inout_old = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout_old.csv')
   df_playlist_artist_inout = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')

   # WHEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   actual = sa.identify_artist_to_be_out(df_playlist_artist_inout_old, df_playlist_artist_inout)
   actual['date'] = actual['date'].astype("string")
   nb_malformated_date = len(actual[~actual.date.str.contains("(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})\\+(\\d{2}):(\\d{2})$",regex=True)].date.value_counts())

   # THEN
   expected = 0
   
   assert nb_malformated_date == expected

def test_identify_artist_to_be_out_correct_datetime_value():
   # GIVEN 
   df_playlist_artist_inout_old = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout_old.csv')
   df_playlist_artist_inout = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')
   current_datetime =  datetime.strptime(datetime.now().strftime(date_format), date_format)

   # WHEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   returned_datetime = sa.identify_artist_to_be_out(df_playlist_artist_inout_old, df_playlist_artist_inout)['date'].iat[0]
   returned_datetime = datetime.strptime(returned_datetime.to_pydatetime().strftime(date_format), date_format)

   # THEN
   expected_date_difference_in_days = abs((current_datetime - returned_datetime).days)
   print(expected_date_difference_in_days)
   print(returned_datetime)
   print(current_datetime)

   assert expected_date_difference_in_days >=0 & expected_date_difference_in_days<1

def test_identify_artist_to_be_in_test_dataframe_without_datetime():
   # GIVEN 
   df_playlist_artist_inout_old = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout_old.csv')
   df_playlist_artist_inout = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')

   # WHEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   actual = sa.identify_artist_to_be_in(df_playlist_artist_inout_old, df_playlist_artist_inout)[['artist_id','artist_name','playlist_id','playlist_name','status']]
   actual.to_csv(path_sample_datas + 'actual.csv')
   
   # THEN
   expected = pd.read_csv(path_sample_datas + 'df_artist_to_be_in.csv')[['artist_id','artist_name','playlist_id','playlist_name','status']]

   assert np.array_equal(expected.values, actual.values) == True

def test_identify_artist_to_stay_test_dataframe_without_datetime():
   # GIVEN 
   df_playlist_artist_inout_old = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout_old.csv')
   df_playlist_artist_inout = pd.read_csv(path_sample_datas + 'df_playlist_artist_inout.csv')

   # WHEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   actual = sa.identify_artist_to_stay(df_playlist_artist_inout_old, df_playlist_artist_inout)[['artist_id','artist_name','playlist_id','playlist_name','status']]

   # THEN
   expected = pd.read_csv(path_sample_datas + 'df_artist_to_stay.csv')[['artist_id','artist_name','playlist_id','playlist_name','status']]

   assert np.array_equal(expected.values,actual.values) == True

def test_spotify_get_datas_from_playlists():
   # GIVEN 
   sa = spotify_analyse.spotify_analyse(['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz'])
   
   # WHEN 
   sa.get_datas_from_playlists()
   actual = sa.df

   # THEN

   assert not actual.empty


# pour lancer les test sdirectement ici, il faut changer le path
# path_sample_datas = os.getcwd() + '\\spotify-data-engineer\\tests\\samples\\'
# test_identify_artist_to_be_out_test_dataframe_without_datetime()
# test_identify_artist_to_be_out_correct_datetime_format()
# test_identify_artist_to_be_out_correct_datetime_value()
# test_spotify_analyse_private_function_create_dataset_playlist_artist_inout()