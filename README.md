# Spotify Exercice

## projet en mode jupyter

Le projet a été initialement réalisé en mode jupyter avec une connexion postgresql.

Les étapes pour faire fonctionner le jupyter sont dans le fichier [jupyter](./jupyter/spotify_claude.ipynb)

Les points saillants : 

* il faut créer des variables d'environement SPOTIFY_CLIENT_ID et SPOTIFY_CLIENT_SECRET, voir jupyter

* il faut avoir une base de données postgresql et renseigner les variables d'environnements associées

Le "lancer tout" permet de lancer les traitements et d'alimenter la base de données

## projet en mode dataiku

Voir les captures d'écran [dataiku](./dataiku/Exercice_dataiku.pptx)

## projet en mode python

* il faut créer des variables d'environement SPOTIFY_CLIENT_ID et SPOTIFY_CLIENT_SECRET, voir [jupyter](./jupyter/spotify_claude.ipynb)
* il faut avoir une base de données postgresql et renseigner les variables d'environnements

### installer le projet

```sh
poetry install
```

ou dans mon cas windows

```sh
python -p poetry install
```

## Execution

```sh
### configuration
playlist_ids=['79pUFoqsqWvOyqtO6ZCkrU', '2e2dR6QrjM3sCroLHprQQz']
sa = spotify_analyse(playlist_ids)

### récupération des populatrités
df_artist_popularity = sa.retrieve_artist_popularity()
# sauvegarde des populatrités
sa.store_artist_popularity(df_artist_popularity)

### récupération des artistes entree/sortie
df_artist_inout = sa.retrieve_artist_in_out()
### sauvegarde des artistes entree/sortie
sa.store_playlist_artist_inout_into_db(df_artist_inout)
```

### tests unitaires

cas exemple testés : fonction privée, variable privée, fonctions

```sh
pytest
```

 ou dans mon cas windows

```sh
python -m pytest
```

### couverture de test

#### Lancement

```sh
coverage run -m pytest && coverage report -m
```

 ou dans mon cas windows

```sh
python -m coverage run -m pytest && python -m coverage report -m
```

#### Couverture actuelle

Les fonctions appelant la bdd ne sont pas testés, ce qui explique la faible couverure de tests, certains warning sont causés par la lib externe spotipy

```sh
------ 8 passed, 3 warnings in 9.44s -----
Name                                           Stmts   Miss  Cover   Missing
----------------------------------------------------------------------------
src\spotify_data_engineer\__init__.py              0      0   100%
src\spotify_data_engineer\spotify_analyse.py     187     94    50%   31-60, 64-87, 92-141, 164-165, 177-178, 188-201, 219-233, 236-241, 250, 293-299, 303-311, 315-323, 327-334
tests\__init__.py                                  0      0   100%
tests\test_spotify_analyse.py                     83      4    95%   40-46
----------------------------------------------------------------------------
TOTAL                                            270     98    64%
```

## Restitution des données

### entrées/sorties d'artiste par playlist

il est aussi possible d'utliser la fonction **get_artist_popularity_by_date**

```python
df_artist_in_out = pd.read_sql("SELECT * FROM playlist_artist_inout where date > '1980-09-06 04:28:57+00:00' AND date < '2023-09-06 04:28:57+00:00' AND status != 'stay'", engine)
```

### popularité dans le temps d'un artiste

```python
df_artist_popularity_read = pd.read_sql('SELECT * FROM artist_popularity', engine)
```

## TODOS

beaucoup de travail encore....

quelques pistes :

* découper en classes le projet
* revoir les configuration de connexions bdd en doublons/triplons
* tests unitaires des bdds
* revoir l'utilisation de jupyter en me^me temps que du code pur
* arriver à avoir un environnement qui marche (ne pas utiliser windows?)
* arriver à activer le debugger python
* revoir les insertions en bdd et le to_csv qui ne marche pas bien sur mon poste
* faire fonctionner correctement poetry et pytest
