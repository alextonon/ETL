import pandas as pd

from extract.extract_affluences import AttendanceExtractor

class AttendanceTransformer() :
    def __init__(self):
        pass

    def transform_data_capacite(self,df_capacite) :
        """
        Traitement du dataframe contenant les informations sur les capacités d'hébergement de chaque
        commune. Les données obtenues sont encodées selon une nomenclature propre à l'INSEE, on va
        donc nettoyer ces informations pour qu'elles apparaissent le plus clairement possible dans
        le dataframe, et supprimer les colonnes inutiles.
        Args:
            df_capacite (pd.DataFrame): DataFrame résultant de l'appel à la fonction
        extract.extract_affluences.extract_data_capacite.
        Returns:
            df_capacite (pd.DataFrame): DataFrame nettoyé.
        """

        # On enlève toutes les colonnes correspondant à des valeurs que l'on a déjà filtrées
        # et qui ne nous intéressent pas (FREQ = annuelle, L_STAY = longueur du séjour, ...)
        df_capacite.drop(columns=['TOUR_MEASURE','FREQ','TIME_PERIOD','UNIT_LOC_RANKING','L_STAY'], inplace=True)

        # La colonne GEO contient des codes dont on extrait le code_insee pour chaque commune 
        df_capacite['insee_code'] = df_capacite["GEO"].str.split("-").str[-1]
        df_capacite['dept_code'] = df_capacite['insee_code'].str[:2]

        # La colonne ACTIVITY encode le type d'hébergement, on va rajouter une colonne activity_type
        # qui va donner explicitement le type d'hébergement
        dict_activity_types = {'I551' : 'Hôtel',
                            'I552' : 'Hébergement touristique',
                            'I552A' : 'Village vacances',
                            'I552B' : 'Résidence de tourisme',
                            'I552C' : 'Auberge - Centre sportif',
                            'I553' : 'Terrain de camping'}
        
        df_capacite['activity_type'] = df_capacite['ACTIVITY'].map(dict_activity_types)

        df_capacite.rename(columns={"ACTIVITY" : "id_activity",
                                    "OBS_VALUE_NIVEAU" : "capacity"}, inplace=True)

        df_capacite.drop(columns=['GEO'], inplace=True)

        df_capacite = df_capacite[['insee_code','dept_code','id_activity','activity_type','capacity']]

        # On ne va garder que les lignes des hôtels et campings, car on a pas les données des autres
        # pour les nombres de nuitées
        df_capacite = df_capacite[df_capacite['id_activity'].isin(['I551', 'I553'])]

        return df_capacite


    def transform_data_nb_nuitees(self,df_nb_nuitees) :
        """
        Traitement du dataframe contenant les informations sur les nombres de nuitées de chaque
        département. Les données obtenues sont encodées selon une nomenclature propre à l'INSEE, on va
        donc nettoyer ces informations pour qu'elles apparaissent le plus clairement possible dans
        le dataframe, et supprimer les colonnes inutiles.
        De plus, on voudra récupérer des données de nombre de nuitées par mois dans chaque département,
        pour les hôtels et les campings. Or, si pour les hôtels on les a bien, pour les campings, on a
        seumlement les aggrégats annuels. On va donc estimer le nombre de nuitées mensuels dans les
        campings, en supposant que la répartition au cours des mois de l'année est la même que dans les
        hôtels : 
            Nb_nuitees_mois_camping = Nb_nuitees_an_camping * Nb_nuitees_mois_hotel / Nb_nuitees_an_hotel
        Args:
            df_nb_nuitees (pd.DataFrame): DataFrame résultant de l'appel à la fonction
        extract.extract_affluences.extract_data_nb_nuitees.
        Returns:
            df_nb_nuitees (pd.DataFrame): DataFrame nettoyé, où une ligne = un nombre de nuitées pour
        un département donné, en un mois donné, sur un type d'hébergement donné.
        """

        # On enlève les colonnes inutiles
        df_nb_nuitees.drop(columns=['TOUR_RESID','HOTEL_STA','TERRTYPO','TOUR_MEASURE','UNIT_LOC_RANKING','OBS_STATUS',
                                        'OBS_STATUS_FR','CONF_STATUS','UNIT_MULT','DECIMALS'], inplace=True)
        
        df_nb_nuitees.rename(columns={"FREQ" : "temporal_rate",
                                    "ACTIVITY" : "id_activity",
                                    "TIME_PERIOD" : "time_period",
                                    "OBS_VALUE_NIVEAU" : "nb_nights",
                                    }, inplace=True)

        # On récupère le code INSEE de chaque département
        df_nb_nuitees['dept_code'] = df_nb_nuitees["GEO"].str.split("-").str[-1]

        # On rajoute une colonne activity_type comme pour les capacités
        dict_activity_types = {'I55' : 'Hébergement',
                            'I551' : 'Hôtel',
                            'I552' : 'Hébergement touristique',
                            'I552A_I552C' : 'Village vacances - Auberge - Centre sportif',
                            'I552B' : 'Résidence de tourisme',
                            'I553' : 'Terrain de camping'}
        
        df_nb_nuitees['activity_type'] = df_nb_nuitees['id_activity'].map(dict_activity_types)

        df_nb_nuitees.drop(columns=['GEO'], inplace=True)

        df_nb_nuitees = df_nb_nuitees[['temporal_rate','dept_code','time_period','id_activity','activity_type','nb_nights']]

        # Estimation des nombres de nuitées mensuels pour les campings
        # On commence par séparer les données mensuelles/annuelles et campings/hôtels
        df_hotels_month = df_nb_nuitees[(df_nb_nuitees['id_activity'] == 'I551') & (df_nb_nuitees['temporal_rate'] == 'M')].copy()
        df_hotels_year = df_nb_nuitees[(df_nb_nuitees['id_activity'] == 'I551') & (df_nb_nuitees['temporal_rate'] == 'A')].copy()
        df_campings_year = df_nb_nuitees[(df_nb_nuitees['id_activity'] == 'I553') & (df_nb_nuitees['temporal_rate'] == 'A')].copy()

        df_hotels_month.drop(columns=['temporal_rate'],inplace=True)
        df_hotels_year = df_hotels_year.rename(columns={'nb_nights': 'nb_nights_hotel_year'}).drop(columns=['temporal_rate', 'id_activity'])
        df_campings_year = df_campings_year.rename(columns={'nb_nights': 'nb_nights_camping_year'}).drop(columns=['temporal_rate', 'id_activity'])

        # On récupère l'année (ici on aura que 2024)
        df_hotels_month['year'] = df_hotels_month['time_period'].str[:4]
        df_hotels_year['year'] = df_hotels_year['time_period'].astype(str)
        df_campings_year['year'] = df_campings_year['time_period'].astype(str)

        df_hotels_year.drop(columns=['time_period'],inplace=True)
        df_campings_year.drop(columns=['time_period'],inplace=True)

        # On merge ales données mensuelles des hôtels, annuelles des hôtels, et annuelles des campings
        df_calc = df_hotels_month.merge(df_hotels_year, on=['dept_code','year'], how='left')
        df_calc = df_calc.merge(df_campings_year, on=['dept_code','year'], how='left')

        df_calc = df_calc[df_calc['nb_nights_hotel_year'] > 0]

        # On calcule, pour chaque mois, le nb de nuitées des campings à partir du nb de nuitées annuel,
        # du nombre de nuitées annuel des hôtels et du nombre de nuitées des hôtels dans le mois
        df_calc['nb_nights_camping_month'] = (
            df_calc['nb_nights_camping_year'] * df_calc['nb_nights'] / df_calc['nb_nights_hotel_year']
                                            )

        print(df_calc[['dept_code','time_period','nb_nights','nb_nights_hotel_year','nb_nights_camping_year']].head(12))

        # On récupère les données mensuelles des campings ainsi calculées
        df_campings_month = df_calc[['dept_code','time_period','nb_nights_camping_month']].copy()
        df_campings_month['id_activity'] = 'I553'
        df_campings_month['activity_type'] = 'Camping'
        df_campings_month['temporal_rate'] = 'M'
        df_campings_month = df_campings_month.rename(columns={'nb_nights_camping_month': 'nb_nights'})

        df_hotels_month.drop(columns=['year'],inplace=True)

        # On concatène les données mensuelles des hôtels et des campings dans df_final
        df_final = pd.concat([
                                df_hotels_month,
                                df_campings_month
                            ], ignore_index=True)
        
        df_final = df_final.drop(columns=['temporal_rate'])

        # On s'assure qu'on a bien uniquement des hôtels et des campings
        df_final = df_final[df_final['id_activity'].isin(['I551', 'I553'])]

        return df_final


    def creation_dataframe_affluences(self,df_capacite, df_nb_nuitees) :
        """
        Création du dataframe d'affluences qui servira à l'algorithme global Voyage Voyage. Le but est 
        de fournir, pour chaque cluster spatial (cf  transform.clusterizer.py), un nombre de 
        touristes hébergés dans chaque type d'hébergement, pour chaque mois de l'année.
        Le but de cette démarche sera de déterminer dans quelles zones il y a le plus et le moins de
        touristes à chaque période de l'année.
        Pour ce faire, on va combiner les deux dataframes de capacité d'hébergement (précis à 
        l'échelle de la commune) et de nombre de nuitées (précis à l'échelle du département), en 
        supposant que chaque mois, les nuitées au sein d'un département sont réparties au sein des
        communes proportionnellement à leur capacité d'hébergement :
            Nb_nuitees_ville = Nb_nuitees_dep * Cap_ville / Cap_dep
        On va ensuite attribuer chaque commune au cluster auquel elle appartient,
        et sommer les nombres de touristes obtenus au sein de chaque cluster, chaque mois.
        Il faudra ensuite prendre garde à l'analyse que l'on peut en faire, car les clusters ne sont
        pas tous aussi densément peuplés.
        """

        df_capacite['capacity_dept'] = df_capacite.groupby(['dept_code','id_activity'])['capacity'].transform('sum')

        colonnes_capacite = ['insee_code','dept_code','id_activity','activity_type','capacity','capacity_dept']
        colonnes_nuitees = ['dept_code','time_period','id_activity','nb_nights']


        df_affluences = df_capacite[colonnes_capacite].merge(df_nb_nuitees[colonnes_nuitees],
                                                            on=['dept_code', 'id_activity'],
                                                            how='left')
        
        df_affluences.rename(columns={"nb_nights" : "nb_nights_dept",
                                    "capacity" : "capacity_city"}, inplace=True)
        
        df_affluences['nb_nights_city'] = df_affluences['nb_nights_dept'] * 1000 * df_affluences['capacity_city'] / df_affluences['capacity_dept']

        return df_affluences
    

    def affluences_cluster(self, df_affluences, df_communes) :
        """
        Aggrège les données du dataframe d'affluences par bassin d'emploi (i.e. cluster), en sommant
        les valeurs des lignes correspondant à des communes appartenant au bassin concerné.
        Args:
            df_affluences (pd.DataFrame): DataFrame des affluences par mois et par commune, obtenu
            en sortie de la fonction creation_dataframe_affluences
            df_communes (pd.DataFrame): DataFrame contenant les informations relatives à chaque commune,
            dont le bassin d'emploi en particulier.
        Returns:
            df_affluences_cluster (pd.DataFrame) : Dataframe final de la partie affluences
        """

        df_affluences_cluster = pd.DataFrame()

        df_affluences['zone_emploi'] = None

        for i in range (len(df_affluences_cluster)) :

            insee_code = df_affluences.loc[i, 'insee_code']

            zone_emploi = df_communes.loc[df_communes['code_insee'] == insee_code, 'zone_emploi'].iloc[0]

            df_affluences.loc[i, 'zone_emploi'] = zone_emploi

        
        df_affluences_cluster = (df_affluences.groupby(
                            ['zone_emploi', 'id_activity', 'activity_type', 'time_period'], as_index=False)
                            .agg({
                                'capacity_city': 'sum',
                                'nb_nights_city': 'sum'
                            })
                            )
        

        df_affluences_cluster.rename(columns={
                        'capacity_city': 'capacity_zone',
                        'nb_nights_city': 'nb_nights_zone'
                    }, inplace=True)
        

        return df_affluences_cluster



if __name__ == "__main__" :

    Extractor = AttendanceExtractor()

    Transformer = AttendanceTransformer()

    df_capacite = Extractor.extract_data_capacite()

    df_capacite = Transformer.transform_data_capacite(df_capacite)

    print(df_capacite.head())

    df_nb_nuitees = Extractor.extract_data_nb_nuitees()

    df_nb_nuitees = Transformer.transform_data_nb_nuitees(df_nb_nuitees)

    print(df_nb_nuitees.head())

    df_affluences = Transformer.creation_dataframe_affluences(df_capacite, df_nb_nuitees)

    print(df_affluences.head())

    # A compléter avec la partie d'Antonin
    #  df_communes = 

    # df_affluence_cluster = Transformer.affluences_cluster(df_affluences, df_communes)

    # print(df_affluence_cluster)
