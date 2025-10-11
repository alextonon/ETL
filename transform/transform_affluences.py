import pandas as pd

import extract.extract_affluences as extract_affluences

def transform_data_capacite(df_capacite) :

    df_capacite.drop(columns=['TOUR_MEASURE','FREQ','TIME_PERIOD','UNIT_LOC_RANKING','L_STAY'], inplace=True)

    df_capacite['insee_code'] = df_capacite["GEO"].str.split("-").str[-1]

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

    df_capacite = df_capacite[['insee_code','id_activity','activity_type','capacity']]

    return df_capacite


def transform_data_taux_hebergements(df_taux_hebergements) :

    df_taux_hebergements.drop(columns=['FREQ','TOUR_RESID','HOTEL_STA','TERRTYPO','TOUR_MEASURE','UNIT_LOC_RANKING','OBS_STATUS',
                                       'OBS_STATUS_FR','CONF_STATUS','UNIT_MULT','DECIMALS'], inplace=True)
    
    df_taux_hebergements['insee_code'] = df_taux_hebergements["GEO"].str.split("-").str[-1]

    dict_activity_types = {'I55' : 'Hébergement',
                           'I551' : 'Hôtel',
                           'I552' : 'Hébergement touristique',
                           'I552A_I552C' : 'Village vacances - Auberge - Centre sportif',
                           'I552B' : 'Résidence de tourisme',
                           'I553' : 'Terrain de camping'}
    
    df_taux_hebergements['activity_type'] = df_taux_hebergements['ACTIVITY'].map(dict_activity_types)

    df_taux_hebergements.rename(columns={"ACTIVITY" : "id_activity",
                                         "TIME_PERIOD" : "mois",
                                        "OBS_VALUE_NIVEAU" : "accomodation_rate",
                                }, inplace=True)

    df_taux_hebergements.drop(columns=['GEO'], inplace=True)

    df_taux_hebergements = df_taux_hebergements[['insee_code','mois','id_activity','activity_type','accomodation_rate']]

    return df_taux_hebergements


if __name__ == "__main__" :

    #df_capacite = extract_affluences.extract_data_capacite()

    #df_capacite = transform_data_capacite(df_capacite)

    #print(df_capacite.head())

    df_taux_hebergements = extract_affluences.extract_data_taux_remplissage()

    df_taux_hebergements = transform_data_taux_hebergements(df_taux_hebergements)

    print(df_taux_hebergements.head())