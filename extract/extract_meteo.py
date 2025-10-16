import requests
import pandas as pd


class MeteoExtractor:
    def __init__(self):
        pass


    def extract(self):
        # Logic to extract meteorological data from the data source
        pass

    def get_brute_dataset(self):
        # Logic to return the raw dataset

        # On utilise la fonction export, en ajoutant une selection sur les variables qui nous interessent  
        # Cela est possible grâce à la lecture de la doc
        data_source = (
            "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "donnees-synop-essentielles-omm/exports/csv?"
            "select=date,latitude,longitude,nom_dept,code_dep,codegeo,pres,nom,tc,rr3,rr24,raf10"
            "&lang=fr"
            "&timezone=Europe%2FParis"
            "&use_labels=true"
            "&delimiter=%3B"
        )

        print("📄 Reading meteorological data from CSV...")

        self.df = pd.read_csv(data_source, sep=';')

