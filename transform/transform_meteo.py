import pandas as pd
import numpy as np


class TransformMeteo:
    def __init__(self, df_meteo):
        self.df_meteo = df_meteo
    
    def process_data(self):
        # Logic to process the extracted data
        self.df = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')

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

        return self.df_mensuel

    def get_station_table(self):
        self.df.groupby([
            "Latitude", "Longitude",
            "department (name)", "department (code)",
            "communes (code)", "Nom"]).unique()
        

    def create_cluster_table(self, cluster_table):
        cluster_table["nearest_meteo_station"] = None

        for cluster in cluster_table.index:
            print(f"Processing cluster {cluster}")
            
            long = cluster_table.loc[cluster, 'longitude_centre']
            lat = cluster_table.loc[cluster, 'latitude_centre']

            nearest_station = self.find_nearest_station(long, lat)

            cluster_table.loc[cluster, 'nearest_meteo_station'] = nearest_station
        self.df_clusters = cluster_table
        return self.df_clusters


    def find_nearest_station(self, long, lat):
        """
        Version vectorisée pour trouver la station météo la plus proche.
        """
        df = self.df_mensuel.drop_duplicates(subset=["communes (code)"])
        distances = np.sqrt((df["Longitude"] - long)**2 + (df["Latitude"] - lat)**2)
        nearest_idx = distances.idxmin()
        return df.loc[nearest_idx, "communes (code)"]

    
if __name__ == "__main__":
    df_meteo = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')
    transformer = TransformMeteo(df_meteo)
    df_cleaned = transformer.process_data()
    
    df_cluster = pd.read_csv("data/cluster_mapping.csv")
    cluster_table = transformer.create_cluster_table(df_cluster)
    cluster_table.to_csv("data/cluster_table.csv", index=False)

    df_cleaned.to_csv("data/meteo_cleaned.csv", index=False)
