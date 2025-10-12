import pandas as pd
import requests, os
import json
import time

def extract_data_insee(api_url):
    """
    Fonction qui effectue une requête sur l'API de l'INSEE, et renvoie le dataframe recherché.
    Source : https://portail-api.insee.fr/catalog/api/a890b735-159c-4c91-90b7-35159c7c9126/doc
    Args:
        api_url (url): url de la requête que l'on veut effectuer
    Returns:
        df (pd.DataFrame): DataFrame python résultant de la requête
    """

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
    """
    Fonction qui extrait les données de capacités d'hébergements touristiques dans toute la France.
    Ces données ont été actualisées en 2025, et pour chaque commune, on a accès au nombre de lits
    proposés pour différentes catégories : hôtels, campings, ...
    Pour les nombres de nuitées on a uniquement accès aux hôtels et aux campings donc on va faire
    de même pour les capacités.
    L'appel à l'API de l'INSEE ne permet d'exporter que 10000 lignes à la fois, or le dataframe
    concerné (DS_TOUR_CAP) en comporte bien plus, on va donc requêter l'API de manière séparée
    pour chaque département, et concaténer les résultats obtenus dans le dataframe qui sera retourné.
    Args: 
        None
    Returns:
        df_capacite (pd.DataFrame): DataFrame python contenant, pour chaque ville et chaque type
    d'hébergement, le nombre de lits disponibles.
    """

    # Identifiants des départements dans la nomenclature INSEE
    liste_id_departements = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                            "11", "12", "13", "14", "15", "16", "17", "18", "19", "21",
                            "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B",
                            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
                            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
                            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
                            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
                            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
                            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
                            "90", "91", "92", "93", "94", "95"]

    liste_api_url_capacite_departement = []

    # On construit une liste d'URLs pour chaque département
    for id_departement in liste_id_departements :

        liste_api_url_capacite_departement.append(f"https://api.insee.fr/melodi/data/DS_TOUR_CAP?TOUR_MEASURE=PLACE&UNIT_LOC_RANKING=_T&L_STAY=_T&ACTIVITY=I551&ACTIVITY=I553&GEO=2025-DEP-{id_departement}*COM")

    df_capacite = pd.DataFrame()

    # Pour chaque département, on appelle l'URL correspondante pour remplir df_capacite
    for i in range(len(liste_api_url_capacite_departement)) :

        df_capacite_region = extract_data_insee(liste_api_url_capacite_departement[i])
        df_capacite = pd.concat([df_capacite, df_capacite_region], ignore_index=True)
        print(f"Département {liste_id_departements[i]} ajouté")

    return df_capacite


def extract_data_nb_nuitees() :
    """
    Fonction qui extrait les nombres de nuitées des différents types d'hébergements (hôtel,
    camping, ...), pour chaque mois de l'année 2024, et pour l'année 2024 au global,
    dans chaque département de France.
    Au niveau du département, on a accès uniquement aux hôtels et aux campings, et pas aux autres
    types d'hébergements malheureusement. De plus, les données mensuelles sont uniquement
    disponibles pour les hôtels et pas pour les campings, donc on va estimer ces dernières en
    récupérant les données annuelles et en répartissant selon l'importance des mois (cf plus tard).
    A l'origine on aurait voulu récupérer les taux d'occupation plutôt, mais il manque des données
    dans la source INSEE (on a uniquement les hôtels au niveau départemental), donc on a préféré
    récupérer les nombres de nuitées.
    Args:
        None
    Returns:
        df_nb_nuitees (pd.DataFrame): DataFrame python contenant les informations souhaitées.
    """

    api_url_nb_nuitees = "https://api.insee.fr/melodi/data/DS_TOUR_FREQ?FREQ=A&FREQ=M&TOUR_RESID=_T&HOTEL_STA=_T&TERRTYPO=_T&TOUR_MEASURE=NUI&UNIT_LOC_RANKING=_T&TIME_PERIOD=2024&TIME_PERIOD=2024-01&TIME_PERIOD=2024-02&TIME_PERIOD=2024-03&TIME_PERIOD=2024-04&TIME_PERIOD=2024-05&TIME_PERIOD=2024-06&TIME_PERIOD=2024-07&TIME_PERIOD=2024-08&TIME_PERIOD=2024-09&TIME_PERIOD=2024-10&TIME_PERIOD=2024-11&TIME_PERIOD=2024-12&GEO=DEP"

    df_nb_nuitees = extract_data_insee(api_url_nb_nuitees)

    return df_nb_nuitees





if __name__ == '__main__' :


    df_capacite = extract_data_capacite()
    print(len(df_capacite))
