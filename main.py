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


#from extract.transform_data import 
#from extract.load_data import 

def main():

    """Run the complete ETL pipeline"""

    print("--- Starting ETL Pipeline...")
    print("=" * 50)


    #### ----- COMMUNES ---- ####
    print("--- Starting Town ETL Pipeline...")
    print("=" * 50)

    #### -- Town Extract
    Town_Extract = TownExtractor()

    ### à changer avec la fonction qui importe depuis le site puis sauvegarde 
    df_town = pd.read_csv("data/communes-france-2025.csv", sep=",")


    #### -- Town Transform
    Town_Transformer = TownTransformer(df_town)
    df_towncleaned = Town_Transformer.clean_data()
    df_towncleaned.to_csv("data/communes_france_cleaned.csv", index=False)

    cluster_mapping = Town_Transformer.create_cluster_mapping()
    cluster_mapping.to_csv("data/cluster_mapping.csv")

    print("Données nettoyées et sauvegardées dans 'data/communes_france_cleaned.csv'")

    print(cluster_mapping)
    


    #### ----- AFFLUENCE ---- ####

    print("--- Starting Attendance ETL Pipeline...")
    print("=" * 50)
    
    #### -- Attendance Extract 
    Attendance_Extractor = AttendanceExtractor()

    df_capacite = Attendance_Extractor.extract_data_capacite()
    df_nb_nuitees = Attendance_Extractor.extract_data_nb_nuitees()

    #### -- Affluence Transform
    Attendance_Transformer = AttendanceTransformer()

    df_capacite = Attendance_Transformer.transform_data_capacite(df_capacite)
    df_nb_nuitees = Attendance_Transformer.transform_data_nb_nuitees(df_nb_nuitees)

    df_affluences = Attendance_Transformer.creation_dataframe_affluences(df_capacite, df_nb_nuitees)

    print(df_affluences.head())

    df_affluence_cluster = AttendanceTransformer.affluences_cluster(df_affluences, cluster_mapping)

    # A compléter avec la partie d'Antonin
    #  df_communes = 

    # df_affluence_cluster = Transformer.affluences_cluster(df_affluences, df_communes)

    # print(df_affluence_cluster)

  









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
    main()