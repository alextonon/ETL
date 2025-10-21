# ETL Project - Voyage Voyage

MAURETTE Audrey / LE SECQ Antonin / MAIGNAN Alexandre / TONON Alexandre / CLAICH Octave

## Présentation du projet

Ce repository Git a pour vocation de développer une pipeline ETL dans le cadre de notre projet de start-up "Voyage Voyage". Le but de notre start-up est de proposer aux touristes une aide à la décision dans le cadre de la planification de leurs vacances, en se basant sur leurs envies et sur les caractéristiques de chaque lieu touristique français.

## Organisation du repository

Notre projet utilise plusieurs sources de données, qui sont quasiment indépendantes les unes des autres, ainsi les phases d'extraction et de transformation des données sont effectuées de manière distincte pour chaque source :

```
📂/ETL
|------📂/data
        |------📂/data_extracted 
                |------📄 df_capacite_csv
                |------📄 df_nb_nuitees.csv
                |------📄 df_town.csv
        |------📂/data_transformed
                |------📄 cluster_affluence.csv
                |------📄 cluster_mapping.csv
                |------📄 cluster_meteo.csv
                |------📄 communes_france_cleaned.csv
|------📂/extract
        |------📄 __init__.py
        |------📄 extract_affluences.py
        |------📄 extract_communes.py
        |------📄 extract_data_tourisme.py
        |------📄 extract_meteo.py
|------📂/load
        |------📄 database_setup_test.sql
        |------📄 load_data.py
|------📂/transform
        |------📄 __init__.py
        |------📄 transform_affluences.py
        |------📄 transform_communes.py
        |------📄 transform_data_tourisme.py
        |------📄 transform_meteo.py
|------📄 .gitignore
|------📄 main.py
|------📄 Readme.md
|------📄 requirements.txt          
```

## Description des différents dossiers

### Dossier data :

Ce dossier contient des enregistrements au format csv des différents jeux de données utilisés, à différentes étapes de la pipeline. Dans le dossier `data_extracted`, on trouve l'état des jeux de données après la phase d'extraction, et dans le dossier `data_transformed`, on trouve l'état des jeux de données après la phase de transformation. Ces fichiers ont principalement été utilisés à des fins de débuggage, pour ne pas avoir à relancer l'intégralité de la pipeline à chaque fois que l'on veut fixer un problème. On pourra également les utiliser si la phase d'extraction prend trop de temps, mais en pratique, on ne devrait pas en avoir besoin puisqu'en lançant l'intégralité de la pipeline la phase de chargement stocke les données pertinentes dans des tables PostGreSQL.

### Dossier extract :

Ce dossier contient les différents fichiers relatifs à la phase d'extraction des jeux de données :

- Le fichier `extract_affluences.py` implémente les fonctions relatives à l'extraction de données de capacité d'hébergement touristique et de taux de fréquentation, depuis des jeux de données de l'INSEE (Sources : https://catalogue-donnees.insee.fr/fr/catalogue/recherche/DS_TOUR_CAP, https://catalogue-donnees.insee.fr/fr/catalogue/recherche/DS_TOUR_FREQ).

- Le fichier `extract_communes.py` implémente les fonctions relatives à l'extraction de données administratives sur les communes de France, depuis un jeu de données de data.gouv (Source : https://www.data.gouv.fr/datasets/communes-france-1/).

- Le fichier `extract_data_tourisme.py` implémente les fonctions relatives à l'extraction de données relatives aux points d'intérêts touristiques/culturels/sportifs/... de France, depuis un jeu de données du site DataTourisme (Source : https://www.datatourisme.fr/).

- Le fichier `extract_meteo.py` implémente les fonctions relatives à l'extraction de données relatives à la météo des différents départements français, depuis un jeu de données de Météo France (Source : https://public.opendatasoft.com/explore/assets/donnees-synop-essentielles-omm/?flg=fr-fr).


### Dossier load :

Ce dossier contient deux fichiers : 

- Le fichier `database_setup_test.sql` contient les instructions SQL nécessaires à la création des différentes tables qui stockeront les données utiles à notre projet.

- Le fichier `load_data.py` implémente les fonctions relatives à la phase de chargement des différents jeux de données utiles à notre projet. On remplit ainsi les différentes tables SQL avec les données issues des phases d'extraction et de transformation.


### Dossier transform :

Ce dossier contient les différents fichiers relatifs à la phase de transformation des jeux de données :

- Le fichier `transform_communes.py` implémente les fonctions relatives à la transformation des données relatives aux communes de France métropolitaine. Il permet de créer un dataframe contenant les informations sur les clusters (zones d'emplois) qui nous serviront à découper la France en différentes zones d'intérêt, et un dataframe faisant correspondre chaque commune de France avec le cluster auquel elle appartient.

- Le fichier `transform_affluences.py` implémente les fonctions relatives à la transformation des données de capacité touristique et de taux de fréquentation en un dataframe de données d'affluences, qui pour chaque cluster géographique, chaque mois de l'année, et chaque type d'hébergement touristique, donne une estimation du nombre de nuitées passées par des touristes dans un hébergement touristique.

- Le fichier `transform_data_tourisme.py` implémente les fonctions relatives à la transformation des données relatives aux points d'intérêt touristiques du territoire français métropolitain. Il permet de créer un dataframe recensant les différents points d'intérêt français, et un autre dataframe agrégant ces données en les sommant par cluster géographique et par catégorie (musées, monuments, etc...).

- Le fichier `transform_meteo.py` implémente les fonctions relatives à la transformation des données météorologiques françaises. Il permet de créer un dataframe contenant, pour chaque cluster géographique et chaque mois de l'année, les valeurs moyennes observées (sur les 10 dernières années) de pression, température, précipitations par jour, et rafales de vent.


### Fichier `main.py`

Le fichier `main.py` permet de lancer l'exécution complète de la pipeline ETL. Il est possible de partir directement de fichiers enregistrés dans le dossier `/data` pour éviter de prendre trop de temps sur la phase d'extraction (qui est surtout longue pour la météo et les points d'intérêt). Les sources de données dépendant uniquement de celle relative aux communes de France, la pipeline concernant cette dernière doit être lancée en première, les autres peuvent ensuite être lancées dans un ordre quelconque, afin de remplir les différentes tables SQL souhaitées.