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

        if self.df.empty:
            print("⚠️  Pas de données à nettoyer")
            return self.df_town
        
        print(f"🧹 Nettoyage des données...")
        print(f"On commence avec {len(self.df_town)} ")

        #On ne garde que le 'nom_standard' dans pour la dénomination des communes
        df_town.drop(['nom_sans_pronom', 'nom_a', 'nom_de', 'nom_sans_accent','nom_standard_majuscule'], axis=1, inplace=True)

        #On supprime toutes les autres colonnes inutiles pour la suite
        df_town.drop(['typecom', 'typecom_texte', 'canton_code', 'canton_nom', 'reg_code', 'dep_code', 'epci_code', 'epci_nom', 'codes_postaux', 
                 'academie_code', 'academie_nom', 'code_unite_urbaine', 'nom_unite_urbaine', 'taille_unite_urbaine', 'type_commune_unite_urbaine', 
                 'statut_commune_unite_urbaine', 'superficie_hectare', 'altitude_moyenne', 'altitude_minimale','altitude_maximale', 
                 'latitude_centre', 'longitude_centre', 'niveau_equipements_services', 'niveau_equipements_services_texte', 'gentile', 
                 'url_wikipedia', 'url_villedereve'], axis=1, inplace=True)

        #On supprime les communes d'Outre mer
        df_town = df_town[~df_town['reg_nom'].isin(['Mayotte', 'Guyane', 'La Réunion', 'Guadeloupe', 'Martinique'])]

        # On supprime les communes (3) communes sans code postal ni zone emploi dans la base de donnée 
        df_town = df_town.dropna(subset=['code_insee_centre_zone_emploi', 'code_postal'])

        #On ajoute une colonne pour ré-indicer les clusters de communes réalisés selon les zones emploi définies par l'INSEE
        df_town['code_cluster'] = pd.factorize(df_town['code_insee_centre_zone_emploi'])[0] + 1

        return df_town
