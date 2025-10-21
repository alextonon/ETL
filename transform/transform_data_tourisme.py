import pandas as pd
from extract.extract_data_tourisme import DataTourismExtractor



class DataTourismTransformer():
    def __init__(self, df_tourism, df_cluster) -> None:
        self.df_tourism = df_tourism
        self.df_cluster = df_cluster

        self.df_DataTourisme = pd.DataFrame()

        # A garder score cacher qui compte sans un poids du client
        Logement = ['Hotel', 'BedAndBreakfast', 'HotelRestaurant', 'Hostel', 'CampingAndCaravanning',
                    'Accommodation', 'HotelTrade', 'RentalAccommodation', 'CollectiveAccommodation', 'TableHoteGuesthouse',
                    "AccommodationProduct", 'Guesthouse', 'House'] # LodgingBusiness = logement qui accepte les buisness

        # A garder score cacher qui compte sans un poids du client
        tourism_center = ["TouristInformationCenter"]

        # Garder
        Nourriture = ["FoodEstablishment", 'Restaurant', 'CafeOrCoffeeShop', 'IceCreamShop', 'Bakery']

        # Garder
        Event = ['SaleEvent', 'TheaterEvent', 'Event', 'Festival', 'MusicEvent', "SportsEvent", 'TraditionalCelebration', 'ShowEvent', 'ChildrensEvent',
                'Concert', 'Exhibition', 'LocalAnimation', 'Rambling']

        # A garder score cacher qui compte sans un poids du client
        transport = ['Transport', 'TrainStation', 'BusStation', 'Transporter', 'Airport', 'TaxiCompany'] # Transport = principalement des ports/ BusStation = gare routière

        # Garder
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
            "Logement": Logement,
            "Tourism_center": tourism_center,
            "Nourriture": Nourriture,
            "Event": Event,
            "Transport": transport,
            "Activités": activités,
            "Sport": Sport,
            "Sport_hiver": Sport_hiver,
            "Balade": Balade,
            "Park": Park,
            "Magasin": Magasin,
            "Culture": Culture,
            "Sortie_soir": Sortie_soir,
        }

    def clean_data(self):
        """
        Fonction permettant de nétoyer les donnée brut du dataset DataTourisme. Cette fonction réalise plusieurs
        nettoyages tels que la suppression de colonnes et de lignes inutiles, la reclassification des points of interest (POI),
        ou encore l'ajout de colonnes pour une comprehension plus simple du dataset

        Returns:
            df (pd.DataFrame): DataFrame nettoyé.
        """

        if self.df_tourism.empty:
            print("⚠️  Pas de données à nettoyer")
            return self.df_tourism
        
        print(f"🧹 Nettoyage des données...")
        print(f"On commence avec {len(self.df_tourism)} ")
        
        # On fait un copy pour ne pas modifier l'originale
        df = self.df_tourism.copy()

        ### On supprime les données qui sont inutiles pour notre algorithme de décision et dont les champs sont principalement vide. ###
        df = df.drop(['Periodes_regroupees', 'Covid19_mesures_specifiques', 'Contacts_du_POI', 'Classements_du_POI', 'SIT_diffuseur'], axis=1, errors="ignore")

        ### On récupère uniquement les catégorie de POI (orignialement noyyer dans une url) ###
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('/').str[-1]
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('#').str[-1]

        ### Ici, seul les données sur le code postale, la catégorie du POI ainsi que le nom du POI sont important 
        # pour l'algorithme, c'est pour cela que nous enlevont les ligne avec des case vides sur ces features ###
        df = df.dropna(subset=["Nom_du_POI", "Categories_de_POI", 'Code_postal_et_commune'])

        ### La colonne Code postale et commune comporte trois information que nous allon répartire dans trois colonne différente pour plus 
        # de clarter. Nous pouvons extraire le code postale, le numéro du département ainsi que le nom de la commune ###

        col_index = df.columns.get_loc("Code_postal_et_commune")

        # On récupère dans un dataframe temporaire les données importante
        df_temp = df["Code_postal_et_commune"].str.replace('#', ' ', regex=False).str.replace('+', '', regex=False).str.split(n=1, expand=True)

        ### Passage des dates en format datetime ###
        df["Date_de_mise_a_jour"] = pd.to_datetime(df["Date_de_mise_a_jour"])

        ### On supprime les catégorie de POI qui ne représente pas vraiment des déstination touristique selon une liste "cat_to_keep" ###
        df = df[df["Categories_de_POI"].isin(self.cat_to_keep)].copy()

        ###  On redéfini les catégorie de POI avec des catégorie plus globale ###
        def find_category(cat):
            """ 
            Fonction permettant de trouver la catégorie dans laquelle la sous-catégorie de POI est contenue
            """
            for key, values in self.categorie_dict.items():
                if cat in values:
                    return key
            return "Autre"  # si aucune correspondance trouvée

        df.insert(3, 'Categorie_simplifiee', df["Categories_de_POI"].apply(find_category)) 

        ### Suppression des oublons en fonction du nombre de nan dans leur colonnes ###
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

        ### On récupère un cléf primaire pour la table a partir de l'URI id du POI ###
        df.insert(0, 'ID', df["URI_ID_du_POI"].str.split('/').str[-1])
        df = df.dropna(subset=["ID"])


        ### On convertis les code postaux en floatant ###
        df['code_postal'] = df_temp[0]
        df['code_postal'] = df['code_postal'].astype(float)

        self.df_cluster = self.df_cluster.drop_duplicates(subset="code_postal", keep="first")
        

        # On fusionne sur les codes postaux  ⬇️ (application directe sur df)
        df = df.merge(
            self.df_cluster[['code_postal', 'code_cluster']], 
            on='code_postal', 
            how='left' 
        )


        print(f'Il reste {len(df)} data après nettoyage.') 

        # code_cluster en 2e colonne
        cols = ['ID', 'code_cluster', 'Nom_du_POI', 'Categories_de_POI', 'Categorie_simplifiee', 'Latitude', 
                'Longitude','Date_de_mise_a_jour', 'Description', 'URI_ID_du_POI']
        df = df[cols]

        self.df_DataTourisme = df.copy()

        self.df_DataTourisme.reset_index(drop=True, inplace=True)

        self.df_DataTourisme.dropna(subset=["code_cluster"], inplace=True)

        return df


if __name__ == "__main__":
    df_cluster = pd.read_csv('data/data_transformed/communes_france_cleaned.csv')
    df_tourism = pd.read_csv("data/data_extracted/df_datatourisme.csv")

    tourism_transformer = DataTourismTransformer(df_tourism, df_cluster)

    df_dataTourisme = tourism_transformer.clean_data()

    df_dataTourisme.to_csv('data/data_transformed/datatourism_cleaned.csv')
    print("Data enregistrée avec succès")

