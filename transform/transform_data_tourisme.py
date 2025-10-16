import pandas as pd
from ..extract.extract_data_tourisme import DataTourismExtractor

class DataTourismTransformer():
    def __init__(self, df_tourism, cat_to_keep, categorie_dict, df_cluster) -> None:
        self.df_tourism = df_tourism
        self.cat_to_keep = cat_to_keep
        self.categorie_dict = categorie_dict
        self.df_cluster = df_cluster

        self.df_DataTourisme = pd.DataFrame()

    def clean_data(self):
        """
        Fonction permettant de nétoyer les donnée brut du dataset DataTourisme. Cette fonction réalise plusieur
        nettoyage tel que la suppression de colonne et de ligne inutile, la reclassification des point of intrest (POI),
        ou encore l'ajout de colonnes pour un comprehension plus simple du dataset

        Returns:
            df (pd.DataFrame): DataFrame nettoyé.
        """

        if self.df_tourism.empty:
            print("⚠️  Pas de données a néttoyer")
            return self.df_tourism
        
        print(f"🧹 Nettoyage des données...")
        print(f"On commence avec {len(self.df_tourism)} ")
        
        # On fait un copy pour ne pas modifier l'originale
        df = self.df_tourism.copy()


        ### On supprime les données qui sont inutiles pour notre algorithme de décision et dont les champs sont principalement vide. ###
        df.drop(['Periodes_regroupees', 'Covid19_mesures_specifiques', 'Contacts_du_POI', 'Classements_du_POI', 'SIT_diffuseur'], axis=1)


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
        df_temp = df["Code_postal_et_commune"].str.split('#', expand=True)
        df_temp.columns = ["Code_postale", "Commune"]
        df_temp["Département"] = df_temp["Code_postale"].str[0:2].astype(int)

        df.drop(columns=["Code_postal_et_commune"], inplace=True)

        # On inserre les colonne créer
        df.insert(col_index, "Département", df_temp["Département"])
        df.insert(col_index + 1, "Code_postale", df_temp["Code_postale"])
        df.insert(col_index + 2, "Commune", df_temp["Commune"])



        ### Passage des dates en format datetime ###
        df["Date_de_mise_a_jour"] = pd.to_datetime(df["Date_de_mise_a_jour"])

        ### On supprime les catégorie de POI qui ne représente pas vraiment des déstination touristique selon une liste "cat_to_keep" ###


        df = df[df["Categories_de_POI"].isin(self.cat_to_keep)].copy()

        ###  On redéfini les catégorie de POI avec des catégorie plus globale ###
        def find_category(self, cat):
            """ 
            Fonction permettant de trouver la catégory dans laquelle la sous-catégorie de POI est contenue
            
            Args:
                cat (String) : sous catégorie du POI
            
            Retunr:
                key (String) :nom de la catégorie du POI
            
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


        ### On récupère un cléf primaire pour la table a partire de l'URI id du POI ###

        df.insert(0, 'ID', df["URI_ID_du_POI"].str.split('/').str[-1])


        ### On réindexe en fonction du département ###

        df = df.sort_values(by=['Département'], ascending=[True]).reset_index(drop=True)


        ### On convertis les code postaux en floatant ###
        
        df['Code_postale'] = df['Code_postale'].astype(float)


        ### On récupère les cluster id depuis la table des cluster en fonction des codes postaux ###

        df_cluster = self.df_cluster.rename(columns={'code_postal': 'Code_postale'})

        # On fusionne sur les codes postaux
        df_merged = df.merge(
            df_cluster[['Code_postale', 'zone_emploi']], 
            on='Code_postale', 
            how='left' 
        )

        # On renome la colonne pour plus de clarter
        df = df_merged.rename(columns={'zone_emploi': 'Cluster_id'})

        print(f'Il reste {len(df)} data après nettoyage.') 

        self.df_DataTourisme = df.copy()

        return df
    
    def to_csv(self):
        """
        Fonction pour sauvegarder le dataframe en csv
        """
        self.df_DataTourisme.to_csv('DataTourismClean.csv', index=False)

    def compute_score(self, dict_poids, cluster):
        """Focntion permettant de calculer le score d'un cluster en fonction de la liste des poids sur les catégories 
        détermiber par les choix de l'utilisateur
        Args:
            dict_poids (Dict) : dictionnaire avec le nom de la catégorie et le poid associer
            cluster (int) : le cluster id à calculer
            df (Dataframe) : dataframe de DataTourisme nétoyer
        Return:
            score (int) : score d'attractiviter du cluster"""

        df_count = self.df_tourism[self.df_tourism['Cluster_id'] == cluster].copy()

        score = 0

        for categorie, poid in dict_poids.items():
            df_subset = df_count[df_count['Categorie_simplifiee'] == categorie]
            
            score += poid * df_subset.shape[0]

        return score


### Cluster ###

if __name__ == "__main__":

    list_df = ["datatourisme-reg-ara.csv", "datatourisme-reg-bfc.csv", "datatourisme-reg-bre.csv",
        "datatourisme-reg-cor.csv", "datatourisme-reg-cvl.csv", "datatourisme-reg-gde.csv",
        "datatourisme-reg-hdf.csv", "datatourisme-reg-naq.csv", "datatourisme-reg-nor.csv",
        "datatourisme-reg-idf.csv",  "datatourisme-reg-occ.csv", "datatourisme-reg-pac.csv",
        "datatourisme-reg-pdl.csv"]
    
    extractor = DataTourismExtractor(list_df)
    
    # extractor.extract_csv()
    df_tourism = extractor.extract_data()

    print(df_tourism)

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



    Liste_to_keep = ['Winery', 'NightClub', 'BistroOrWineBar', 'BrasserieOrTavern', 'Church', 'RemarkableBuilding', 'TechnicalHeritage', 
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

    categorie_dict = {
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

    # tourism_transformer = DataTourismTransformer(df_tourism, Liste_to_keep, categorie_dict, df_cluster)

    # df_dataToursime = tourism_transformer.clean_data()


