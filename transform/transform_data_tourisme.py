import pandas as pd
from extract.extract_data_tourisme import DataTourismExtractor



class DataTourismTransformer():
    def __init__(self, df_tourism, df_cluster) -> None:
        self.df_tourism = df_tourism
        self.df_cluster = df_cluster

        self.df_DataTourisme = pd.DataFrame()

        # A garder score caché qui compte sans un poids du client
        Logement = ['Hotel', 'BedAndBreakfast', 'HotelRestaurant', 'Hostel', 'CampingAndCaravanning',
                    'Accommodation', 'HotelTrade', 'RentalAccommodation', 'CollectiveAccommodation', 'TableHoteGuesthouse',
                    "AccommodationProduct", 'Guesthouse', 'House'] # LodgingBusiness = logement qui accepte les buisness

        # A garder score caché qui compte sans un poids du client
        tourism_center = ["TouristInformationCenter"]

        # A Garder
        Nourriture = ["FoodEstablishment", 'Restaurant', 'CafeOrCoffeeShop', 'IceCreamShop', 'Bakery']

        # A Garder
        Event = ['SaleEvent', 'TheaterEvent', 'Event', 'Festival', 'MusicEvent', "SportsEvent", 'TraditionalCelebration', 'ShowEvent', 'ChildrensEvent',
                'Concert', 'Exhibition', 'LocalAnimation', 'Rambling']

        # A garder score caché qui compte sans un poids du client
        transport = ['Transport', 'TrainStation', 'BusStation', 'Transporter', 'Airport', 'TaxiCompany'] # Transport = principalement des ports/ BusStation = gare routière

        # A Garder
        activités = ['Product', 'Hammam', 'AmusementPark', 'Landform', 'Casino',  'BowlingAlley', 'RailBike', 'MiniGolf', 'AdventurePark'
                    'BalneotherapyCentre', 'SummerToboggan', 'NauticalCentre',
                    'TastingProvider', 'ActivityProvider',  'Rental', 'Trampoline', 'EquestrianCenter', 'EquipmentRental',
                    "Tour", 'LeisureSportActivityProvider', 'Practice', 'EntertainmentAndEvent', 'MegalithDolmenMenhir', 'TrainingWorkshop', 'TeachingFarm',
                    "CulturalActivityProvider", 'Cinematheque', 'Visit', 'WalkingTour'] # Product = visite en tt genre / Landform = Plage /  Tour = sentier de rando

        # A garder
        Sport = ['SportsAndLeisurePlace', 'OrderedList',  'GolfCourse', 'ClimbingWall', 'TennisComplex', 
                "CyclingTour", 'TerrainPark', 'FrontonBelotaCourt', 'SportsClub'] # OrderedList = rando + VTT

        # A garder
        Sport_hiver = ['CrossCountrySkiTrail', 'DownhillSkiRun', 'DownhillSkiResort', 'CrossCountrySkiResort']


        # A garder
        Balade = ['NaturalHeritage', 'ServiceArea','EducationalTrail', 'ViaFerrata', 'RomanPath', 'LevyOrDike',] # ServiceArea = Site pour observer les étoile / EducationalTrail = balade / LevyOrDike = balade de barrage

        # A garder
        Park = ['Park', 'CivicStructure', 'PicnicArea', 'ParkAndGarden'] # CivicStructure = Parcoure de santé, air de jeux

        Magasin = ['CoveredMarket', "Store", 'Market', 'LocalProductsShop', 'BoutiqueOrLocalShop']


        # A garder
        Culture = ['Church', 'RemarkableBuilding', 'TechnicalHeritage', 'Cloister', 'Cathedral', 'FortifiedCastle', 'Palace', 
                    'Fort', 'ReligiousSite', 'Temple', "Ruins", "RemembranceSite", 'Dungeon', 'DefenceSite', 'Abbey', 'Convent', 
                    'Monastery', 'Collegiate', 'Tower', 'Fountain','Chapel', 'Mine', 'Bridge', 'Basilica', 'Chartreuse',
                    'BuddhistTemple', 'Mosque', 'Aqueduct', 'ArcheologicalSite',"Castle", 'Synagogue', 'FortifiedSet', 'Citadel', 
                    "RemarkableHouse", "Commanderie", 'Marina', 'Bastide', 'Lighthouse','Arena', 'LocalBusiness', 'Aquarium', 
                    "CulturalSite", 'Theater', "Library", 'Museum'] # RemembranceSite = Memoriale,...

        # A garder
        Sortie_soir = ['Winery', 'NightClub', 'BistroOrWineBar', 'BrasserieOrTavern']

        self.cat_to_keep = ['Winery', 'NightClub', 'BistroOrWineBar', 'BrasserieOrTavern', 'Church', 'RemarkableBuilding', 'TechnicalHeritage', 
                'Cloister', 'Cathedral', 'FortifiedCastle', 'Palace', 'BalneotherapyCentre', 'SummerToboggan', 'NauticalCentre',
                    'Fort', 'ReligiousSite', 'Temple', "Ruins", "RemembranceSite", 'Dungeon', 'DefenceSite', 'Abbey', 'Convent', 
                    'Monastery', 'Collegiate', 'Tower', 'Fountain','Chapel', 'Mine', 'Bridge', 'Basilica', 'Chartreuse',
                    'BuddhistTemple', 'Mosque', 'Aqueduct', 'ArcheologicalSite',"Castle", 'Synagogue', 'FortifiedSet', 'Citadel', 
                    "RemarkableHouse", "Commanderie", 'Marina', 'Bastide', 'Lighthouse','Arena', 'LocalBusiness', 'Aquarium', 
                    "CulturalSite", 'Theater', "Library", 'Museum', 'Hotel', 'BedAndBreakfast', 'HotelRestaurant', 'Hostel', 'CampingAndCaravanning',
                    'Accommodation', 'HotelTrade', 'RentalAccommodation', 'CollectiveAccommodation', 'TableHoteGuesthouse',
                    "AccommodationProduct", 'Guesthouse', 'House', "TouristInformationCenter", "FoodEstablishment", 'Restaurant', 'CafeOrCoffeeShop', 'IceCreamShop', 'Bakery',
                    'SaleEvent', 'TheaterEvent', 'Event', 'Festival', 'MusicEvent', "SportsEvent", 'TraditionalCelebration', 'ShowEvent', 'ChildrensEvent',
                    'Concert', 'Exhibition', 'LocalAnimation', 'Rambling', 'Transport', 'TrainStation', 'BusStation', 'Transporter', 'Airport', 'TaxiCompany',
                    'Product', 'Hammam', 'AmusementPark', 'Landform', 'Casino',  'BowlingAlley', 'RailBike', 'MiniGolf', 'AdventurePark'
                    'TastingProvider', 'ActivityProvider',  'Rental', 'Trampoline', 'EquestrianCenter', 'EquipmentRental',
                    "Tour", 'LeisureSportActivityProvider', 'Practice', 'EntertainmentAndEvent', 'MegalithDolmenMenhir', 'TrainingWorkshop', 'TeachingFarm',
                    "CulturalActivityProvider", 'Cinematheque', 'Visit', 'WalkingTour', 'SportsAndLeisurePlace', 'OrderedList',  'GolfCourse', 'ClimbingWall', 'TennisComplex', 
                    "CyclingTour", 'TerrainPark', 'FrontonBelotaCourt', 'SportsClub', 'CrossCountrySkiTrail', 'DownhillSkiRun', 'DownhillSkiResort', 'CrossCountrySkiResort',
                    'NaturalHeritage', 'ServiceArea','EducationalTrail', 'ViaFerrata', 'RomanPath', 'LevyOrDike','Park', 'CivicStructure', 'PicnicArea', 'ParkAndGarden',
                    'CoveredMarket', "Store", 'Market', 'LocalProductsShop', 'BoutiqueOrLocalShop']

        self.categorie_dict = {
            "logements": Logement,
            "centres_de_tourisme": tourism_center,
            "nourriture": Nourriture,
            "evenements": Event,
            "transports": transport,
            "activités": activités,
            "sports": Sport,
            "sports_hiver": Sport_hiver,
            "balades": Balade,
            "parcs": Park,
            "magasins": Magasin,
            "culture": Culture,
            "sorties_soir": Sortie_soir,
        }

    def clean_data(self):
        """
        Fonction permettant de nettoyer les données brut du dataset DataTourisme. Cette fonction réalise plusieurs
        nettoyages tels que la suppression de colonnes et de lignes inutiles, la reclassification des points of interest (POI),
        ou encore l'ajout de colonnes pour une compréhension plus simple du dataset

        Returns:
            df (pd.DataFrame): DataFrame nettoyé.
        """

        if self.df_tourism.empty:
            print("⚠️  Pas de données à nettoyer")
            return self.df_tourism
        
        print(f"🧹 Nettoyage des données...")
        print(f"On commence avec {len(self.df_tourism)} ")
        
        # On fait un copy pour ne pas modifier le dataframe original
        df = self.df_tourism.copy()

        ### On supprime les données qui sont inutiles pour notre algorithme de décision et dont les champs sont principalement vides. ###
        df = df.drop(['Periodes_regroupees', 'Covid19_mesures_specifiques', 'Contacts_du_POI', 'Classements_du_POI', 'SIT_diffuseur'], axis=1, errors="ignore")

        ### On récupère uniquement les catégories de POI (orignialement noyées dans une url) ###
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('/').str[-1]
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('#').str[-1]

        ### Ici, seules les données sur le code postal, la catégorie du POI ainsi que le nom du POI sont importants
        # pour l'algorithme, c'est pour cela que nous enlevons les lignes avec des cases vides sur ces features ###
        df = df.dropna(subset=["Nom_du_POI", "Categories_de_POI", 'Code_postal_et_commune'])

        ### Les colonnes Code postal et commune comportent trois informations que nous allons répartir dans trois colonnes différentes pour plus 
        # de clarté. Nous pouvons extraire le code postal, le numéro du département ainsi que le nom de la commune ###

        col_index = df.columns.get_loc("Code_postal_et_commune")

        # On récupère dans un dataframe temporaire les données importantes
        df_temp = df["Code_postal_et_commune"].str.replace('#', ' ', regex=False).str.replace('+', '', regex=False).str.split(n=1, expand=True)
        # df_temp = df["Code_postal_et_commune"]

        df_temp.columns = ["code_postal", "Commune"]
        # df['Code_postale'] = df['Code_postale'].astype(str).str.extract('(\d+)')[0].astype(int)
        df_temp["Département"] = df_temp["code_postal"].str[0:2].astype(int)

        df.drop(columns=["Code_postal_et_commune"], inplace=True)

        # On insère les colonnes créées
        df.insert(col_index, "Département", df_temp["Département"])
        df.insert(col_index + 1, "code_postal", df_temp["code_postal"])

        ### Passage des dates en format datetime ###
        df["Date_de_mise_a_jour"] = pd.to_datetime(df["Date_de_mise_a_jour"])

        ### On supprime les catégories de POI qui ne représentent pas vraiment des destinations touristiques selon une liste "cat_to_keep" ###
        df = df[df["Categories_de_POI"].isin(self.cat_to_keep)].copy()

        ###  On redéfinit les catégories de POI avec des catégories plus globales ###
        def find_category(cat):
            """ 
            Fonction permettant de trouver la catégorie dans laquelle la sous-catégorie de POI est contenue
            """
            for key, values in self.categorie_dict.items():
                if cat in values:
                    return key
            # return "Autre"  # si aucune correspondance trouvée

        df.insert(3, 'Categorie_simplifiee', df["Categories_de_POI"].apply(find_category)) 

        ### Suppression des doublons en fonction du nombre de nan dans leurs colonnes ###
        # On compte le nombre de nan dans une ligne
        df["nb_nan"] = df.isna().sum(axis=1)
        # On trie par nombre de nan
        df = df.sort_values(by=["Nom_du_POI", "nb_nan"], ascending=[True, True])
        # On supprime les doublons en gardant la première ligne
        df = df.drop_duplicates(subset="Nom_du_POI", keep="first")
        # On supprime la colonne temporaire
        df = df.drop(columns="nb_nan")
        # On réindexe 
        df = df.reset_index(drop=True)

        ### On récupère une clé primaire pour la table a partir de l'URI id du POI ###
        df.insert(0, 'ID', df["URI_ID_du_POI"].str.split('/').str[-1])
        df = df.dropna(subset=["ID"])


        ### On convertit les codes postaux en floatant ###
        df['code_postal'] = df['code_postal'].astype(float)

        self.df_cluster = self.df_cluster.drop_duplicates(subset="code_postal", keep="first")
        

        # On fusionne sur les codes postaux
        df = df.merge(
            self.df_cluster[['code_postal', 'code_cluster']], 
            on='code_postal', 
            how='left' 
        )

        ### On réindexe en fonction du département ###
        df = df.sort_values(by=['Département'], ascending=[True]).reset_index(drop=True)

        # code_cluster en 2e colonne
        cols = ['ID', 'code_cluster', 'Nom_du_POI', 'Categories_de_POI', 'Categorie_simplifiee', 'Latitude', 
                'Longitude','Date_de_mise_a_jour', 'Description', 'URI_ID_du_POI']
        df = df[cols]

        df.dropna(subset=["code_cluster"], inplace=True)

        df.reset_index(drop=True, inplace=True)

        df.sort_values(by='code_cluster')

        self.df_DataTourisme = df.copy()

        print(f'Il reste {len(df)} data après nettoyage.') 

        ### On crée un nouveau dataframe pour garder uniquement le nombre de POI par cluster par activité ###

        pivot_df = df.groupby(["code_cluster", "Categorie_simplifiee"]).size().unstack(fill_value=0)
        score_cluster_POI_df = pivot_df.rename_axis(columns=None).reset_index() 

        ##Renommage des colonnes : enlever les majuscules pour faciliter les requêtes SQL
        df.columns = [col.lower() for col in df.columns]
        
        return df, score_cluster_POI_df


if __name__ == "__main__":
    df_cluster = pd.read_csv('data/data_transformed/communes_france_cleaned.csv')
    df_tourism = pd.read_csv("data/data_extracted/df_datatourisme.csv")

    tourism_transformer = DataTourismTransformer(df_tourism, df_cluster)

    df_dataTourisme, df_score_POI_cluster = tourism_transformer.clean_data()

    df_dataTourisme.to_csv('data/data_transformed/datatourism_cleaned.csv', index = False)

    df_score_POI_cluster.to_csv('data/data_transformed/datatourism_score_cluster.csv', index = False)
    print("Data enregistrée avec succès")