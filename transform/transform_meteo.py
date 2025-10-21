import pandas as pd
import numpy as np


class TransformMeteo:
    def __init__(self):
        self.df = None
        self.df_mensuel = None
        self.df_station_table = None
        self.df_cluster_meteo = None
    
    def process_data(self, df_meteo_brut):
        # Logic to process the extracted data
        self.df = df_meteo_brut.copy()

        # On tolère l'absence de colonnes (pas d'erreur si manquantes)
        cols_to_drop = ["Rafale sur les 10 dernières minutes","Précipitations dans les 3 dernières heures"]
        self.df.drop(columns=[c for c in cols_to_drop if c in self.df.columns], inplace=True, errors="ignore")

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
                "Rafales sur une période": "mean"
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
        Version vectorisée pour trouver la station météo la plus proche.
        """
        df = df_meteo.drop_duplicates(subset=["communes (code)"])
        distances = np.sqrt((df["Longitude"] - long)**2 + (df["Latitude"] - lat)**2)
        nearest_idx = distances.idxmin()
        return df.loc[nearest_idx, "communes (code)"]

    def link_clusters_with_meteo(self, df_cluster_table, df_mensuel=None):
        """
        Si df_mensuel est fourni, on l'utilise. Sinon, on utilise self.df_mensuel (dataset intermédiaire possible).
        """
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
            'Rafales sur une période': 'rafales_sur_une_période'
        }
        self.df_cluster_meteo.rename(columns={k:v for k,v in rename_map.items() if k in self.df_cluster_meteo.columns}, inplace=True)

        # Réordonner si toutes les colonnes sont là
        wanted = [
            'code_cluster','mois','pression_station','température',
            'précipitations_24_dernières_heures','rafales_sur_une_période'
        ]
        existing = [c for c in wanted if c in self.df_cluster_meteo.columns]
        self.df_cluster_meteo = self.df_cluster_meteo[[c for c in wanted if c in existing] + [c for c in self.df_cluster_meteo.columns if c not in wanted]]

        self.df_cluster_meteo.sort_values(by=[c for c in ['code_cluster','mois'] if c in self.df_cluster_meteo.columns], inplace=True)

        return self.df_cluster_meteo



if __name__ == "__main__":
    transformer = TransformMeteo()

    df_meteo_brut = pd.read_csv("data/donnees-synop-essentielles-omm.csv", sep=';')

    df_mensuel = transformer.process_data(df_meteo_brut)

    df_mensuel.to_csv("data/data_transformed/meteo_mensuel.csv", index=False)

    df_cluster = pd.read_csv("data/cluster_mapping.csv")
    df_cluster_meteo = transformer.link_clusters_with_meteo(df_cluster) 
    df_cluster_meteo.to_csv("data/data_transformed/cluster_meteo.csv", index=False)

    print(df_mensuel.head())