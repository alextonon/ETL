import pandas as pd
import numpy as np


class TransformMeteo:
    def __init__(self):
        self.df = None
        self.df_mensuel = None
        self.df_station_table = None
        self.df_cluster_meteo = None
    
    def process_data(self, df_meteo_brut):
        """
        Fonction qui transforme les données météorologiques du projet, en ne gardant que les colonnes
        pertinentes, et en agrégeant les données par commune (on moyenne chaque grandeur sur les 10
        années à disposition, pour chaque station météo).
        Args:
            df_meteo_brut (pd.DataFrame): DataFrame contenant les données météorologiques brutes
        Returns:
            df_mensuel (pd.DataFrame): DataFrame nettoyé.
        """

        print("--- Starting brute data cleaning for Meteo...")
        # Logic to process the extracted data
        self.df = df_meteo_brut.copy()

        self.df.dropna(inplace=True)

        # Parsing ISO tolérant
        self.df['Date'] = pd.to_datetime(self.df['Date'], utc=True, errors='coerce')
        self.df.dropna(subset=['Date'], inplace=True)
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
                "Rafale sur les 10 dernières minutes": "mean"
            })
        )

        return self.df_mensuel

    def get_station_table(self, df_source=None):
        """
        Construit une table de stations uniques.
        df_source peut être un df brut ou un df_mensuel; sinon on prend self.df puis self.df_mensuel.
        """
        base = df_source if df_source is not None else (self.df if self.df is not None else self.df_mensuel)
        if base is None:
            raise ValueError("Aucune donnée disponible pour construire la table des stations.")

        cols = [c for c in ["Latitude","Longitude","department (name)","department (code)","communes (code)","Nom"] if c in base.columns]
        self.df_station_table = base[cols].drop_duplicates().reset_index(drop=True)
        return self.df_station_table

    def find_nearest_station(self, long, lat, df_meteo):
        """
        Version vectorisée pour trouver la station météo la plus proche d'un point connu.
        Args:
            long (float): Longitude du point concerné
            lat (float): Latitude du point concerné
            df_meteo (pd.DataFrame): DataFrame contenant les données météorologiques d'intérêt
        Returns:
            df.loc[nearest_idx, "communes (code)"] : code de la commune contenant la station météo
            la plus proche du point concerné.
        """
        df = df_meteo.drop_duplicates(subset=["communes (code)"])
        distances = np.sqrt((df["Longitude"] - long)**2 + (df["Latitude"] - lat)**2)
        nearest_idx = distances.idxmin()
        return df.loc[nearest_idx, "communes (code)"]

    def link_clusters_with_meteo(self, df_cluster_table, df_mensuel=None):
        """
        Fonction faisant le lien entre les données météorologiques et les clusters spatiaux issus
        de la partie transform_communes.
        Args:
            df_cluster_table (pd.DataFrame): Informations sur les clusters spatiaux.
            df_mensuel (pd.DataFrame): DataFrame contenant les données météorologiques de chaque station.
        Si df_mensuel est fourni, on l'utilise. Sinon, on utilise self.df_mensuel (dataset intermédiaire possible).
        Returns:
            df_cluster_meteo (pd.DataFrame): DataFrame final, contenant les données météorologiques
            rassemblées par cluster.
        """
        print("--- Starting linking clusters with meteorological data...")
        df_m = df_mensuel if df_mensuel is not None else self.df_mensuel
        if df_m is None:
            raise ValueError("Aucun df_mensuel disponible. Passe df_mensuel ou appelle process_data().")

        # Table des stations (depuis le mensuel pour rester simple et proche)
        stations = self.get_station_table(df_m)

        df_cluster_table = df_cluster_table.copy()
        df_cluster_table["nearest_meteo_station"] = None

        for cluster in df_cluster_table.index:
            long = df_cluster_table.loc[cluster, 'longitude_centre']
            lat = df_cluster_table.loc[cluster, 'latitude_centre']
            nearest_station = self.find_nearest_station(long, lat, stations)
            df_cluster_table.loc[cluster, 'nearest_meteo_station'] = nearest_station

        self.df_cluster_meteo = df_m.merge(
            df_cluster_table[['nearest_meteo_station', 'code_cluster']],
            left_on='communes (code)',
            right_on='nearest_meteo_station',
            how='inner'
        )

        self.df_cluster_meteo.drop(columns=[c for c in [
            'nearest_meteo_station', 'Latitude', 'Longitude', 'department (code)',
            'department (name)', 'communes (code)', 'Nom'
        ] if c in self.df_cluster_meteo.columns], inplace=True, errors="ignore")

        # Renommer les colonnes selon ton format exact (si présentes)
        rename_map = {
            'Mois': 'mois',
            'Pression station': 'pression_station',
            'Température (°C)': 'température',
            'Précipitations dans les 24 dernières heures': 'précipitations_24_dernières_heures',
            'Rafales sur une période': 'rafales_10_dernières_minutes'
        }
        self.df_cluster_meteo.rename(columns={k:v for k,v in rename_map.items() if k in self.df_cluster_meteo.columns}, inplace=True)

        # Réordonner si toutes les colonnes sont là
        wanted = [
            'code_cluster','mois','pression_station','température',
            'précipitations_24_dernières_heures','rafales_10_dernières_minutes'
        ]
        existing = [c for c in wanted if c in self.df_cluster_meteo.columns]
        self.df_cluster_meteo = self.df_cluster_meteo[[c for c in wanted if c in existing] + [c for c in self.df_cluster_meteo.columns if c not in wanted]]

        self.df_cluster_meteo.sort_values(by=[c for c in ['code_cluster','mois'] if c in self.df_cluster_meteo.columns], inplace=True)

        self.df_cluster_meteo.reset_index(drop=True, inplace=True)

        self.df_cluster_meteo.drop_duplicates(["mois", "code_cluster"], inplace=True)

        return self.df_cluster_meteo



if __name__ == "__main__":
    transformer = TransformMeteo()

    # --- ATTENTION : il faut avoir chargé une première fois le fichier 'df_meteo_brut.csv'.
    # -- Le fichier se télécharge en exécutant extract.extract_meteo.py ou main.py

    print("\n### --- DONNEES FINALES TRANSFORMEES : df_cluster_meteo --- ###\n")
    df_meteo_brut = pd.read_csv("data/data_extracted/df_meteo_brut.csv", sep=';')
    print(df_meteo_brut.head())

    df_mensuel = transformer.process_data(df_meteo_brut)

    df_cluster = pd.read_csv("data/data_transformed/cluster_mapping.csv")
    df_cluster_meteo = transformer.link_clusters_with_meteo(df_cluster) 
    df_cluster_meteo.to_csv("data/data_transformed/cluster_meteo.csv", index=False)

    print("\n### --- DONNEES FINALES TRANSFORMEES : df_cluster_meteo --- ###\n")
    print(df_cluster_meteo.head())