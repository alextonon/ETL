import pandas as pd


class TownTransformer() :
    def __init__(self, df_town):
        self.df_town = df_town


    def clean_data(self):
            """
            Fonction permettant de nettoyer les données brutes du dataset des communes française disponibles sur data.gouv : 
            https://www.data.gouv.fr/datasets/communes-et-villes-de-france-en-csv-excel-json-parquet-et-feather/. 
            Cette fonction réalise plusieurs nettoyages tels que la suppression de colonnes inutiles, la suppression des communes 
            d'Outre mer qui ne sont pas traités dans la démo, la suppression de 3 communes mal renseignées dans le dataset ou encore 
            l'ajout d'une colonne pour ré-indicer la classification en cluster des communes selon leur zone_emploi.

            Returns:
                df_town (pd.DataFrame): DataFrame nettoyé.
            """

            if self.df_town.empty:
                print("--- ⚠️  Pas de données à nettoyer")
                return self.df_town
            
            print("... Cleaning communes dataset.")

            #On ne garde que le 'nom_standard' dans pour la dénomination des communes
            self.df_town.drop(['nom_sans_pronom', 'nom_a', 'nom_de', 'nom_sans_accent','nom_standard_majuscule'], axis=1, inplace=True)

            #On supprime toutes les autres colonnes inutiles pour la suite
            self.df_town.drop(['typecom', 'typecom_texte', 'canton_code', 'canton_nom', 'reg_code', 'dep_code', 'epci_code', 'epci_nom', 'codes_postaux', 
                    'academie_code', 'academie_nom', 'code_unite_urbaine', 'nom_unite_urbaine', 'taille_unite_urbaine', 'type_commune_unite_urbaine', 
                    'statut_commune_unite_urbaine', 'superficie_hectare', 'altitude_moyenne', 'altitude_minimale','altitude_maximale', 
                     'niveau_equipements_services', 'niveau_equipements_services_texte', 'gentile', 
                    'url_wikipedia', 'url_villedereve'], axis=1, inplace=True)

            #On supprime les communes d'Outre mer
            self.df_town = self.df_town[~self.df_town['reg_nom'].isin(['Mayotte', 'Guyane', 'La Réunion', 'Guadeloupe', 'Martinique'])]

            # On supprime les communes (3) communes sans code postal ni zone emploi dans la base de donnée 
            self.df_town = self.df_town.dropna(subset=['code_insee_centre_zone_emploi', 'code_postal'])

            #On ajoute une colonne pour ré-indicer les clusters de communes réalisés selon les zones emploi définies par l'INSEE
            self.df_town['code_cluster'] = pd.factorize(self.df_town['code_insee_centre_zone_emploi'])[0] + 1

            self.df_town.drop(["Unnamed: 0"], axis=1, inplace=True)

            return self.df_town

    def create_cluster_mapping(self):
        """
        Fonction permettant de créer un mapping entre les codes des clusters et les zones d'emplois associées.
        Returns:
            cluster_mapping (df): Dictionnaire de mapping entre les codes des clusters et les zones d'emplois.
        """
        if self.df_town.empty:
            print("---⚠️  Pas de données pour créer le mapping")
            return pd.DataFrame(columns=["code_cluster", "code_insee_centre_zone_emploi", "latitude_centre", "longitude_centre"])
        

        self.cluster_mapping = (
            self.df_town.groupby("code_cluster")
            .agg({
                "code_insee_centre_zone_emploi": "first",
                "latitude_centre": "mean",
                "longitude_centre": "mean"
            })
        )

        for cluster_code in self.cluster_mapping.index:
            biggest_town = self.get_biggest_town_cluster(cluster_code)
            self.cluster_mapping.at[cluster_code, 'ville_principale'] = biggest_town['nom_standard']

        self.cluster_mapping.reset_index(inplace=True)

        return self.cluster_mapping
    
    def get_biggest_town_cluster(self, cluster_code):
        """
        Fonction permettant de récupérer la plus grande commune d'un cluster donné.
        Args:
            cluster_code (int): Code du cluster.
        Returns:
            biggest_town (pd.Series): Série contenant les informations de la plus grande commune du cluster.
        """
        if self.df_town.empty:
            print("--- ⚠️  Pas de données pour récupérer la plus grande commune")
            return None

        cluster_towns = self.df_town[self.df_town['code_cluster'] == cluster_code]
        biggest_town = cluster_towns.loc[cluster_towns['population'].idxmax()]

        return biggest_town

if __name__ == "__main__":
    df_town = pd.read_csv("data/data_extracted/df_town.csv", sep=",")
    transformer = TownTransformer(df_town)

    print("\n### --- communes_france_cleaned --- ###\n")
    df_cleaned = transformer.clean_data()
    df_cleaned.to_csv("data/data_transformed/communes_france_cleaned.csv", index=False)
    print(df_cleaned.head(), '\n')

    print("### --- cluster_mapping--- ###\n")
    cluster_mapping = transformer.create_cluster_mapping()
    cluster_mapping.to_csv("data/data_transformed/cluster_mapping.csv", index=False)
    print(cluster_mapping.head())
