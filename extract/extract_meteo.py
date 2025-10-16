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


    def process_data(self):
        # Logic to process the extracted data
        self.df = pd.read_csv("donnees-synop-essentielles-omm.csv", sep=';')

        self.df.drop(columns=["Rafale sur les 10 dernières minutes","Précipitations dans les 3 dernières heures"], inplace=True)

        self.df.dropna(inplace=True)

        self.df['Date'] = pd.to_datetime(self.df['Date'], format="ISO8601", utc=True)
        self.df['Mois'] = self.df['Date'].dt.month
        
        # On construit un gros tcd, avec une méthode adaptée pour la pluie
        self.df_mensuel = (
        self.df.groupby([
            "Latitude", "Longitude",
            "department (name)", "department (code)",
            "communes (code)", "Nom", "Mois"
        ], as_index=False)
        .agg({
            "Pression station": "mean",
            "Température (°C)": "mean",
            "Précipitations dans les 24 dernières heures": "sum",
            "Rafales sur une période": "mean"
        })
        )

    def get_station_table(self):
        self.df.groupby([
            "Latitude", "Longitude",
            "department (name)", "department (code)",
            "communes (code)", "Nom"]).unique()


if __name__ == "__main__":
    extractor = MeteoExtractor()
    
    extractor.process_data()
    df = extractor.df_mensuel

    print(df)
