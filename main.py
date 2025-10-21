"""
Voyage Voyage ETL Pipeline - Simple Version

This script runs the complete ETL pipeline:
1. Extract ... 
2. Clean and transform the data
3. Load the data into PostgreSQL database

Run with: python main.py
"""


import pandas as pd

from extract.extract_affluences import AttendanceExtractor
from extract.extract_communes import TownExtractor
from extract.extract_meteo import MeteoExtractor
from extract.extract_data_tourisme import DataTourismExtractor

from transform.transform_affluences import AttendanceTransformer
from transform.transform_communes import TownTransformer
from transform.transform_meteo import TransformMeteo
from transform.transform_data_tourisme import DataTourismTransformer

from load.load_data import get_connection_string, load_to_database_affluence, load_to_database_clusters, load_to_database_meteo, test_database_connection 
#from load.load_data import verify_data, run_sample_queries

def pipeline_meteo(df_cluster):
    """Run the Meteo ETL pipeline"""
    print("--- Starting METEO ETL Pipeline...")
    print("=" * 50)

    #### -- Meteo Extract 

    meteo_extractor = MeteoExtractor()
    df_meteo = meteo_extractor.get_brute_dataset(local=True)

    df_meteo.to_csv("data/data_extracted/df_meteo_brut.csv")

    #### -- Meteo Transform :

    transformer = TransformMeteo()

    df_mensuel = transformer.process_data(df_meteo)

    df_cluster_meteo = transformer.link_clusters_with_meteo(df_cluster, df_mensuel) 
    df_cluster_meteo.to_csv("data/data_transformed/cluster_meteo.csv", index=False) # Stockage table finale

    print("✅ Meteo ETL Pipeline completed.")

    return df_cluster_meteo

def pipeline_town():
    """Run the Town ETL pipeline"""
    print("--- Starting TOWN ETL Pipeline...")
    print("=" * 50)

    #### -- TOWN Extract

    town_extractor = TownExtractor()
    df_town = town_extractor.extract_data(["communes-france-2025.csv"])

    #### -- Town Transform
    Town_Transformer = TownTransformer(df_town)
    df_towncleaned = Town_Transformer.clean_data()
    df_towncleaned.to_csv("data/data_transformed/communes_france_cleaned.csv", index=False) # Stockage intermédiaire

    cluster_mapping = Town_Transformer.create_cluster_mapping() 
    cluster_mapping.to_csv("data/data_transformed/cluster_mapping.csv") # Stockage table finale

    print("✅ Town ETL Pipeline completed.")
    return df_towncleaned, cluster_mapping

def pipeline_affluences(df_towncleaned):
    """Run the Affluences ETL pipeline"""
    print("--- Starting AFFLUENCE ETL Pipeline...")
    print("=" * 50)
    
    #### -- Attendance Extract 
    Attendance_Extractor = AttendanceExtractor()

    df_capacite = Attendance_Extractor.extract_data_capacite()
    df_nb_nuitees = Attendance_Extractor.extract_data_nb_nuitees()

    df_capacite.to_csv("data/data_extracted/df_capacite.csv", index=False)
    df_nb_nuitees.to_csv("data/data_extracted/df_nb_nuitees.csv", index=False)

    #### -- Affluence Transform
    Attendance_Transformer = AttendanceTransformer()

    df_capacite = Attendance_Transformer.transform_data_capacite(df_capacite)
    df_nb_nuitees = Attendance_Transformer.transform_data_nb_nuitees(df_nb_nuitees)

    df_affluences = Attendance_Transformer.creation_dataframe_affluences(df_capacite, df_nb_nuitees)


    df_affluence_cluster = AttendanceTransformer.affluences_cluster(df_affluences, df_towncleaned)

    df_affluence_cluster.to_csv("data/data_transformed/cluster_affluence.csv", index = False) 
    print("✅ Affluence ETL Pipeline completed.")
    return df_affluence_cluster


    


def main():

    """Run the complete ETL pipeline"""

    print("--- Starting ETL Pipeline...")
    print("=" * 50)


    #### ----- COMMUNES ---- ####
    print("--- Starting TOWN ETL Pipeline...")
    print("=" * 50)

    #### -- TOWN Extract
    Town_Extract = TownExtractor()

    ### à changer avec la fonction qui importe depuis le site puis sauvegarde 
    df_town = pd.read_csv("data/communes-france-2025.csv", sep=",")


    #### -- Town Transform
    Town_Transformer = TownTransformer(df_town)
    df_towncleaned = Town_Transformer.clean_data()
    df_towncleaned.to_csv("data/data_transformed/communes_france_cleaned.csv", index=False)

    cluster_mapping = Town_Transformer.create_cluster_mapping()
    cluster_mapping.to_csv("data/data_transformed/cluster_mapping.csv")

    print("Données nettoyées et sauvegardées dans '/data_transformed")

    
    


    #### ----- AFFLUENCE ---- ####

    print("--- Starting AFFLUENCE ETL Pipeline...")
    print("=" * 50)
    
    #### -- Attendance Extract 
    Attendance_Extractor = AttendanceExtractor()

    df_capacite = Attendance_Extractor.extract_data_capacite()
    df_nb_nuitees = Attendance_Extractor.extract_data_nb_nuitees()

    df_capacite.to_csv("data/data_extracted/df_capacite.csv", index=False)
    df_nb_nuitees.to_csv("data/data_extracted/df_nb_nuitees.csv", index=False)

    #### -- Affluence Transform
    Attendance_Transformer = AttendanceTransformer()

    df_capacite = Attendance_Transformer.transform_data_capacite(df_capacite)
    df_nb_nuitees = Attendance_Transformer.transform_data_nb_nuitees(df_nb_nuitees)

    df_affluences = Attendance_Transformer.creation_dataframe_affluences(df_capacite, df_nb_nuitees)


    df_affluence_cluster = AttendanceTransformer.affluences_cluster(df_affluences, df_towncleaned)

    df_affluence_cluster.to_csv("data/data_transformed/cluster_affluence.csv", index = False)


    
    #### ----- METEO ---- ####

    print("--- Starting METEO ETL Pipeline...")
    print("=" * 50)

    #### -- Meteo Extract 

    MeteoExtractor = MeteoExtractor()
    df_meteo = MeteoExtractor.get_brute_dataset()

    #%%%% trop long ?
    #df_meteo.to_csv("data/data_extracted/df_meteo_brut.csv")


    #### -- Meteo Transform : A MODIFIER

    MeteoTransformer = TransformMeteo(df_meteo)

    #df_meteo_cleaned = transformer.process_data()
    #df_meteo_cleaned.to_csv("data/data_transformed/meteo_cleaned.csv", index=False)
    df_meteo_cleaned = pd.read_csv("data/meteo_cleaned.csv")
    TransformMeteo.df_mensuel = df_meteo_cleaned  

    df_cluster = pd.read_csv("data/cluster_mapping.csv")
    df_cluster_meteo = TransformMeteo.link_clusters_with_meteo(df_cluster)
    df_cluster_meteo.to_csv("data/data_transformed/cluster_meteo.csv", index=False)


    #### ----- DATA TOURISM ---- ####

    print("--- Starting DATA TOURISM ETL Pipeline...")
    print("=" * 50)

    #### -- DataTourism Extract 

    # ...

    #### -- Datatourism Transform : A MODIFIER
    
    
    # ...


    #### LOADING  ####



    if test_database_connection():
        print("\nDatabase connection OK. Ready for data loading!")
        
      
        # Test loading 
        print('========== LOADING CLUSTERS ========== ')
        load_to_database_clusters(cluster_mapping)

        print('========== LOADING METEO ========== ')
        load_to_database_meteo(df_cluster_meteo)

        print('========== LOADING AFFLUENCE ========== ')
        load_to_database_affluence(df_affluence_cluster)

        print('========== LOADING DATATOURISM ========== ')
        #load_to_database_datatourism()

       



    else:
        print("Fix database connection before testing loading functions")




    ##### LOADING : A FAIRE
  
        #verify_data, run_sample_queries








    # --------- Exemple AIRLIFE ---------
    # Step 1: Extract data
   # print("\n=== EXTRACTION ===")
    #print("📥 Extracting data from sources...")
    
    # TODO: Call the extraction functions
    # airports = extract_airports()
    # flights = extract_flights()
    
    # Uncomment the lines above once you've implemented the functions
    #print("⚠️  Extraction functions not yet implemented")
    #return
    
    # Step 2: Transform data
  #  print("\n=== TRANSFORMATION ===")
   # print("🔄 Cleaning and transforming data...")
    
    # TODO: Call the transformation functions
    # clean_airports_data = clean_airports(airports)
    # clean_flights_data = clean_flights(flights)
    # final_airports, final_flights = combine_data(clean_airports_data, clean_flights_data)
    
    # Step 3: Load data
   # print("\n=== LOADING ===")
   # print("💾 Loading data to database...")
    
    # TODO: Call the loading function
    # load_to_database(final_airports, final_flights)
    
    # Step 4: Verify everything worked
  #  print("\n=== VERIFICATION ===")
    #print("✅ Verifying data was loaded correctly...")
    
    # TODO: Call the verification function
    # verify_data()
    
    #print("\n🎉 ETL Pipeline completed!")
    print("=" * 50)

if __name__ == "__main__":
    df_town, df_cluster = pipeline_town()

    print("=" * 50)

    # df_meteo = pipeline_meteo(df_cluster)

    print("=" * 50)
    
    df_affluence = pipeline_affluences(df_town)