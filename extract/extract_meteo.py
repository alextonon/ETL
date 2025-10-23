import requests
import pandas as pd
from io import StringIO


class MeteoExtractor:
    def __init__(self):
        pass

    def extract_data(self, local=False):
        """
        Fonction qui extrait les données météorologiques pertinentes concernant la France
        métropolitaine sur les 10 dernières années, depuis l'API OpenDataSoft.
        Args:
            local (bool): Booléen qui précise si l'extraction doit être effectuée depuis le fichier
        enregistré en local data/donnees-synop-essentielles-omm.csv, ou si la requête à l'API doit
        être effectuée.
        Returns:
            df (pd.DataFrame): DataFrame résultant de l'extraction des données météo.
        """

        # Logic to return the raw dataset

        # On utilise la fonction export, en ajoutant une selection sur les variables qui nous interessent  
        # Cela est possible grâce à la lecture de la doc
        if local:
            print("--- Reading meteorological data from local csv...")
            self.df = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')

        else:
            print("--- Reading meteorological data from remote API...")
            data_source = (
                "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
                "donnees-synop-essentielles-omm/exports/csv?"
                "select=date,latitude,longitude,nom_dept,code_dep,codegeo,pres,nom,tc,rr3,rr24,raf10"
                "&lang=fr"
                "&timezone=Europe%2FParis"
                "&use_labels=true"
                "&delimiter=%3B"
            )

            # Étape intermédiaire : requête HTTP pour vérifier le statut
            response = requests.get(data_source)

            if response.status_code == 200:
                print("--- ✅ HTTP 200 OK: Data successfully retrieved.")
                # Lecture du CSV depuis la réponse brute
                self.df = pd.read_csv(StringIO(response.text), sep=';')
            else:
                print(f"--- Erreur HTTP : statut {response.status_code}")
                return None

            print(f"--- Data successfully loaded. Shape: {self.df.shape}")
            return self.df

        return self.df

        

if __name__ == '__main__' :

    Extractor = MeteoExtractor()

    df_meteo = Extractor.extract_data()

    df_meteo.to_csv("data/data_extracted/df_meteo_brut.csv")

    print(df_meteo.head())
