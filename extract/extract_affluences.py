import pandas as pd
import requests, os
import json
import time

def extract_data_insee(api_url):

    try:
        print("Making API request... (this may take a few seconds)")

        get_data = requests.get(api_url, verify= False)
        time.sleep(2)
        data_from_net = get_data.content
        data = json.loads(data_from_net)

        # Extraction des informations du jeu de données
        title = data['title']['fr']
        identifier = data['identifier']

        #Extraction des observations du jeu de données filtré, sur lesquelles on va boucler
        observations = data['observations']
        extracted_data = []

        #Boucle de lecture des observations dans le json 
        for obs in observations:
            dimensions = obs['dimensions']
            
            # Suivant les jeux de données attributes est présent ou non
            if 'attributes' in obs:
                attributes = obs['attributes']
            else:
                attributes = None

            # Suivant les jeux de données value peut être absent
            if 'value' in obs['measures']['OBS_VALUE_NIVEAU']:
                measures = obs['measures']['OBS_VALUE_NIVEAU']['value']
            else:
                mesures = None
            
            # on rassemble tout dans un objet
            if 'attributes' in obs:
                combined_data = {**dimensions,**attributes, 'OBS_VALUE_NIVEAU': measures}
            else:
                combined_data = {**dimensions, 'OBS_VALUE_NIVEAU': measures}
            
            extracted_data.append(combined_data)

        #Création d'un dataframe python
        df = pd.DataFrame(extracted_data)

        print(f'Jeu de données : {identifier} \nTitre : {title} ')

        return df

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return pd.DataFrame()
    

def extract_data_capacite() :

    liste_id_regions = [11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94]

    liste_api_url_capacite_region = []

    for id_region in liste_id_regions :

        liste_api_url_capacite_region.append(f"https://api.insee.fr/melodi/data/DS_TOUR_CAP?TOUR_MEASURE=PLACE&UNIT_LOC_RANKING=_T&L_STAY=_T&GEO=COM&GEO=2025-REG-{id_region}")

    df_capacite = pd.DataFrame()

    for api_url_capacite_region in liste_api_url_capacite_region :

        df_capacite_region = extract_data_insee(api_url_capacite_region)
        df_capacite = pd.concat([df_capacite, df_capacite_region], ignore_index=True)

    return df_capacite


def extract_data_taux_remplissage() :

    api_url_taux_remplissage = "https://api.insee.fr/melodi/data/DS_TOUR_FREQ?FREQ=M&TOUR_RESID=_T&HOTEL_STA=_T&TERRTYPO=_T&TOUR_MEASURE=PLACE_OCCUPANCY_RATE&UNIT_LOC_RANKING=_T&TIME_PERIOD=2024-01&TIME_PERIOD=2024-02&TIME_PERIOD=2024-03&TIME_PERIOD=2024-04&TIME_PERIOD=2024-05&TIME_PERIOD=2024-06&TIME_PERIOD=2024-07&TIME_PERIOD=2024-08&TIME_PERIOD=2024-09&TIME_PERIOD=2024-10&TIME_PERIOD=2024-11&TIME_PERIOD=2024-12&GEO=DEP"

    df_taux_remplissage = extract_data_insee(api_url_taux_remplissage)

    return df_taux_remplissage





if __name__ == '__main__' :


    df_taux_remplissage = extract_data_taux_remplissage()
    print(df_taux_remplissage.head())
