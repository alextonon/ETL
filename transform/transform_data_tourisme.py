import pandas as pd

class DataTourismTransformer():
    def __init__(self, df_tourism, cat_to_keep, categorie_dict, df_cluster) -> None:
        self.df_tourism = df_tourism
        self.cat_to_keep = cat_to_keep
        self.categorie_dict = categorie_dict
        self.df_cluster = df_cluster

    def clean_data(self):

        if self.df_tourism.empty:
            print("⚠️  No data to clean")
            return self.df_tourism
        
        print(f"🧹 Cleaning  data...")
        print(f"Starting with {len(self.df_tourism)} ")
        
        # Make a copy to avoid modifying the original
        df = self.df_tourism.copy()

        # Drop the usless data
        df.drop(['Periodes_regroupees', 'Covid19_mesures_specifiques', 'Contacts_du_POI', 'Classements_du_POI', 'SIT_diffuseur'], axis=1)


        # On récupère uniquement les catégorie de POI
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('/').str[-1]
        df["Categories_de_POI"] = df["Categories_de_POI"].str.split('#').str[-1]

        # On découpe la colone en 3 pour plus de clareter
        col_index = df.columns.get_loc("Code_postal_et_commune")

        df_temp = df["Code_postal_et_commune"].str.split('#', expand=True)
        df_temp.columns = ["Code_postale", "Commune"]
        df_temp["Département"] = df_temp["Code_postale"].str[0:2].astype(int)

        df.drop(columns=["Code_postal_et_commune"], inplace=True)


        df.insert(col_index, "Département", df_temp["Département"])
        df.insert(col_index + 1, "Code_postale", df_temp["Code_postale"])
        df.insert(col_index + 2, "Commune", df_temp["Commune"])

        # On drop les valeur null
        df = df.dropna(subset=["Nom_du_POI", "Categories_de_POI", 'Latitude', 'Longitude']) # ,"Adresse_postale"]) 

        # Passage des dates en format date
        df["Date_de_mise_a_jour"] = pd.to_datetime(df["Date_de_mise_a_jour"])

        

        # --- Étape 1 : filtrer selon A_garder ---
        df = df[df["Categories_de_POI"].isin(self.cat_to_keep)].copy()

        # --- Étape 2 : ajouter la catégorie simplifiée ---
        def find_category(cat):
            for key, values in categorie_dict.items():
                if cat in values:
                    return key
            return "Autre"  # si aucune correspondance trouvée

        df.insert(3, 'Categorie_simplifiee', df["Categories_de_POI"].apply(find_category)) 


        df["nb_nan"] = df.isna().sum(axis=1)

        # Trier pour que la ligne avec le moins de NaN soit en premier dans chaque groupe
        df = df.sort_values(by=["Nom_du_POI", "nb_nan"], ascending=[True, True])

        # Supprimer les doublons, en gardant la première (celle avec le moins de NaN)
        df = df.drop_duplicates(subset="Nom_du_POI", keep="first")

        # Supprimer la colonne temporaire
        df = df.drop(columns="nb_nan")

        # (optionnel) Réindexer proprement
        df = df.reset_index(drop=True)

        df.insert(0, 'ID', df["URI_ID_du_POI"].str.split('/').str[-1])

        df = df.sort_values(by=['Département'], ascending=[True]).reset_index(drop=True)
        
        df['Code_postale'] = df['Code_postale'].astype(float)

        df_cluster = self.df_cluster.rename(columns={'code_postal': 'Code_postale'})

        # Fusion (jointure) sur le code postal
        df_merged = df.merge(
            df_cluster[['Code_postale', 'zone_emploi']], 
            on='Code_postale', 
            how='left'  # garde tous les POI même sans cluster
        )

        # Renommer la colonne résultante pour plus de clarté
        df_merged = df_merged.rename(columns={'zone_emploi': 'Cluster_id'})

        print(f'Il reste {len(df)} data après nettoyage.') 

        return df

### Cluster ###

if __name__ == "__main__":

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

