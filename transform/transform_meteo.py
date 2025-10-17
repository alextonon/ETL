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
        return self.df_station_table


    def find_nearest_station(self, long, lat):
        """
        Version vectorisée pour trouver la station météo la plus proche.
        """
        df = self.df_mensuel.drop_duplicates(subset=["communes (code)"])
        distances = np.sqrt((df["Longitude"] - long)**2 + (df["Latitude"] - lat)**2)
        nearest_idx = distances.idxmin()
        return df.loc[nearest_idx, "communes (code)"]


    def link_clusters_with_meteo(self, df_cluster_table):
        """
        Associe à chaque cluster la station météo la plus proche et toutes les données météo
        correspondantes, pour chaque mois.
        
        Args:
            df_cluster_table (pd.DataFrame): DataFrame avec colonnes 'nearest_meteo_station' et 'code_cluster'
        
        Returns:
            df_cluster_meteo (pd.DataFrame): DataFrame où chaque ligne correspond à un cluster + mois,
                                            avec toutes les données météo de la station la plus proche
        """
        df_cluster_table["nearest_meteo_station"] = None

        for cluster in df_cluster_table.index:
            print(f"Processing cluster {cluster}")

            long = df_cluster_table.loc[cluster, 'longitude_centre']
            lat = df_cluster_table.loc[cluster, 'latitude_centre']

            nearest_station = self.find_nearest_station(long, lat)

            df_cluster_table.loc[cluster, 'nearest_meteo_station'] = nearest_station


        # Merge sur le code INSEE de la station
        self.df_cluster_meteo = self.df_mensuel.merge(
            df_cluster_table[['nearest_meteo_station', 'code_cluster']],
            left_on='communes (code)',
            right_on='nearest_meteo_station',
            how='inner'
        )

        # On garde la colonne code_cluster, on supprime la colonne doublon
        self.df_cluster_meteo.drop(columns=['nearest_meteo_station', 'Latitude', 'Longitude', 'department (code)',
                                       'department (name)', 'communes (code)', 'Nom'], inplace=True)

        # Le merge conserve toutes les colonnes de df_mensuel, donc chaque mois est conservé
        # Les données sont automatiquement "par mois" grâce à la colonne 'Mois' dans df_mensuel

        self.df_cluster_meteo["ID"] = self.df_cluster_meteo["code_cluster"].astype(str) + "_" + self.df_cluster_meteo["Mois"].astype(str)

        self.df_cluster_meteo.rename(columns={'Pression station': 'Meteo_Pression_station_moyenne',
                                              'Température (°C)': 'Meteo_Temperature_moyenne',
                                              'Précipitations dans les 24 dernières heures': 'Meteo_Precipitations_moyenne',
                                              'Rafales sur une période': 'Meteo_Rafales_moyenne'}, inplace=True)

        return self.df_cluster_meteo


    
if __name__ == "__main__":
    df_meteo = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')
    transformer = TransformMeteo(df_meteo)

    df_meteo_cleaned = transformer.process_data()
    df_meteo_cleaned.to_csv("data/meteo_cleaned.csv", index=False)

    df_cluster = pd.read_csv("data/cluster_mapping.csv")

    df_cluster_meteo = transformer.link_clusters_with_meteo(df_cluster)

    df_cluster_meteo.to_csv("data/cluster_meteo.csv", index=False)
