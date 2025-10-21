import requests
import pandas as pd


class MeteoExtractor:
    def __init__(self):
        pass

    def extract_data(self, local=True):
        # Logic to return the raw dataset

        # On utilise la fonction export, en ajoutant une selection sur les variables qui nous interessent  
        # Cela est possible grâce à la lecture de la doc
        if local:
            print("--- Reading meteorological data from local csv...")
            self.df = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')

        else :
            data_source = (
                "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
                "donnees-synop-essentielles-omm/exports/csv?"
                "select=date,latitude,longitude,nom_dept,code_dep,codegeo,pres,nom,tc,rr24"
                "&lang=fr"
                "&timezone=Europe%2FParis"
                "&use_labels=true"
                "&delimiter=%3B"
            )

            print("--- Reading meteorological data from Meteo France API...")

            self.df = pd.read_csv(data_source, sep=';')

        return self.df

        

if __name__ == '__main__' :

    Extractor = MeteoExtractor()

    df_meteo = Extractor.get_brute_dataset()

    df_meteo.to_csv("data/data_extracted/df_meteo_brut.csv")

    print(df_meteo.head())
