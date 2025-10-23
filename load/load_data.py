"""
Data Loading Module

Ce module s'occupe de : 
---> établir une connexion avec la base de données SQL
---> charger les données nettoyées dans la base de données SQL
        Cela concerne les données sur :
        - les zones géorgraphiques dans la table clusters
        - la météo de chaque zone dans la table meteo
        - la fréquentation de chaque zone dans la table affluence
        - les différents lieux d'intérêt de chaque zone et leur description dans la table datatourisme
        - la quantité de lieux d'intérêt par zone dans la table socretourisme

---> créer une VIEW dans la base de données à partir des tables créées, qui servira d'input pour l'algorithme de recommandation

---> vérifier que les données ont été correctement chargées 

---> réaliser quelques exemples de requêtes sur les tables 

"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from dotenv import load_dotenv
import os




#Charge les variables depuis le fichier .env
load_dotenv()

#Récupère les variables d'environnement dans le fichier .env
DATABASE_CONFIG = {
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME')
}


def get_connection_string():
    """
    Connexion à PostgreSQL à partir des informations contenues dans DATABASE_CONFIG
    """
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def test_database_connection():
        """
        Test database connection without loading data
        """
        print("🔌 Testing database connection...")
        
        connection_string = get_connection_string()
        
        try:
            engine = create_engine(connection_string)
            
            # Try a simple query
            result = pd.read_sql("SELECT 1 as test", engine)
            
            if result.iloc[0]['test'] == 1:
                print("✅ Database connection successful!")
                
                # Check if our tables exist
                tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('meteo', 'affluence', 'clusters', 'datatourisme', 'scoretourisme')
                ORDER BY table_name
                """
                tables = pd.read_sql(tables_query, engine)
                
                if len(tables) == 5:
                    print("✅ Required tables (meteo, affluence, clusters, datatourisme, scoretourisme) exist")
                else:
                    print(f"⚠️  Found {len(tables)} tables, expected 5")
                    print("💡 Run database_setup.sql to create tables")
                
                return True
            else:
                print("❌ Database connection test failed")
                return False
                
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            print("💡 Check your connection settings in DATABASE_CONFIG")
            return False
        



def load_to_database_clusters(clusters_df):
    """
    Load clusters cleaned data into PostgreSQL database
    
    Args:
    clusters_df (pandas.DataFrame): Données nettoyées des clusters 
    """
    print("💾 Loading CLUSTERS data to PostgreSQL database...")

    connection_string = get_connection_string()

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        #Supprime la VIEW dataconsolidée avant de charger de nouvelles données 
        with engine.begin() as conn:  # begin() crée un contexte transactionnel
            conn.execute(text("DROP VIEW IF EXISTS dataconsolidee"))
        
        # Check if clusters_df is not empty before loading
        if not clusters_df.empty :
            clusters_df.to_sql('clusters', engine, if_exists='replace', index=False)
        
        
        if not clusters_df.empty:
            print(f"✅ Loaded {len(clusters_df)} clusters to database")
        else:
            print("ℹ️  No clusters data to load")
        
    except Exception as e:
        print(f"❌ Error loading clusters data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'db_voyagevoyage' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")


def load_to_database_affluence(affluence_df):
    """
    Load affluence cleaned data into PostgreSQL database
    
    Args:
    affluence_df (pandas.DataFrame): Cleaned affluence data
    """
    print("💾 Loading AFFLUENCE data to PostgreSQL database...")

    connection_string = get_connection_string()

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        #Supprime la VIEW dataconsolidée avant de charger de nouvelles données 
        with engine.begin() as conn:  # begin() crée un contexte transactionnel
            conn.execute(text("DROP VIEW IF EXISTS dataconsolidee"))
        
        # Check if affluence_df is not empty before loading
        if not affluence_df.empty :
            affluence_df.to_sql('affluence', engine, if_exists='replace', index=False)
        
        
        if not affluence_df.empty:
            print(f"✅ Loaded {len(affluence_df)} affluence to database")
        else:
            print("ℹ️  No affluence data to load")
        
    except Exception as e:
        print(f"❌ Error loading affluence data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'db_voyagevoyage' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")


def load_to_database_meteo(meteo_df):
    """
    Load meteo cleaned data into PostgreSQL database
    
    Args:
    meteo_df (pandas.DataFrame): Cleaned meteo data
    """
    print("💾 Loading METEO data to PostgreSQL database...")

    connection_string = get_connection_string()

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        #Supprime la VIEW dataconsolidée avant de charger de nouvelles données 
        with engine.begin() as conn:  # begin() crée un contexte transactionnel
            conn.execute(text("DROP VIEW IF EXISTS dataconsolidee"))
        
        
        # Check if meteo_df is not empty before loading
        if not meteo_df.empty :
            meteo_df.to_sql('meteo', engine, if_exists='replace', index=False)
        
        # TODO: Print loading statistics
        #print(f"✅ Loaded {len(meteo_df)} meteo to database")
        if not meteo_df.empty:
            print(f"✅ Loaded {len(meteo_df)} meteo to database")
        else:
            print("ℹ️  No meteo data to load")
        
    except Exception as e:
        print(f"❌ Error loading meteo data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'db_voyagevoyage' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")



def load_to_database_datatourism(datatourism_df, scoretourism_df):
    """
    Load datatourism cleaned data into PostgreSQL database
    
    Args:
    datatourism_df (pandas.DataFrame): Cleaned datatourism data
    """
    print("💾 Loading TOURISM to PostgreSQL database...")

    connection_string = get_connection_string()

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        
        with engine.begin() as conn:  
            #Supprime la VIEW dataconsolidée avant de charger de nouvelles données 
            conn.execute(text("DROP VIEW IF EXISTS dataconsolidee"))
        
        # Check if datatourism_df is not empty before loading
        if not datatourism_df.empty :
            datatourism_df.to_sql('datatourisme', engine, if_exists='replace', index=False)
        if not scoretourism_df.empty :
            scoretourism_df.to_sql('scoretourisme', engine, if_exists='replace', index=False)
        
        if not datatourism_df.empty:
            print(f"✅ Loaded {len(datatourism_df)} datatourism to database")
        else:
            print("ℹ️  No datatourism data to load")

        if not scoretourism_df.empty:
            print(f"✅ Loaded {len(scoretourism_df)} scoretourism to database")
        else:
            print("ℹ️  No scoretourism data to load")
        
    except Exception as e:
        print(f"❌ Error loading datatourism data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'db_voyagevoyage' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")




def create_view():
    """
    Crée la vue dataconsolidee dans la base de données SQL, qui servira d'input pour l'algorithme de recommandation
    """
    engine = create_engine(get_connection_string())

    # Dictionnaires des vues à créer
    views_dict = {
        "dataconsolidee": """
            CREATE VIEW dataconsolidee AS
            SELECT 
                m.code_cluster,
                m.mois, 
                m.pression_station,
                m.température,
                m.précipitations_24_dernières_heures,
                a.nb_nights_camping,
                a.nb_nights_hotel,
                a.capacity_camping,
                a.capacity_hotel,
                st.activités score_activités,
                st.balades score_balades,
                st.centres_de_tourisme score_centres_de_tourisme,
                st.culture score_culture,
                st.evenements score_evenements,
                st.logements score_logements,
                st.magasins score_magasins,
                st.nourriture score_nourriture,
                st.parcs score_parcs,
                st.sorties_soir score_sorties_soir,
                st.sports score_sports,
                st.sports_hiver score_sports_hiver,
                st.transports score_transports
            FROM meteo m
            JOIN affluence a ON m.code_cluster = a.code_cluster AND m.mois = a.mois
            JOIN scoretourisme st ON m.code_cluster = st.code_cluster
            
        """
    }

    with engine.begin() as conn:
        for view_name, view_sql in views_dict.items():
            # Supprimer la vue si elle existe déjà
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name} CASCADE;"))
            
            # Créer la vue
            conn.execute(text(view_sql))
            print(f"✅ Vue '{view_name}' créée !")

            



def verify_data():
    """
    Vérifie que les données ont été correctement chargées en réalisant des requêtes simples
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
                
        
        # Compte le nombre de clusters 
        clusters_count = pd.read_sql("SELECT COUNT(*) as count FROM clusters", engine)
        print(f"📊 Données dans la table clusters: {clusters_count.iloc[0]['count']}")
        
        # Compte le nombre de données météo 
        meteo_count = pd.read_sql("SELECT COUNT(*) as count FROM meteo", engine)
        print(f"📊 Données dans la table météo: {meteo_count.iloc[0]['count']}")

        # Compte le nombre de données affluence
        affluence_count = pd.read_sql("SELECT COUNT(*) as count FROM affluence", engine)
        print(f"📊 Données dans la table affluence: {affluence_count.iloc[0]['count']}")

        # Compte le nombre de données datatourism
        datatourism_count = pd.read_sql("SELECT COUNT(*) as count FROM datatourisme", engine)
        print(f"📊 Données dans la table datatourisme: {datatourism_count.iloc[0]['count']}")

        # Compte le nombre de données scoretourisme
        scoretourism_count = pd.read_sql("SELECT COUNT(*) as count FROM scoretourisme", engine)
        print(f"📊 Données dans la table scoretourisme: {scoretourism_count.iloc[0]['count']}")

        # Compte le nombre de données dans la view dataconsolidee
        dataconsolidee_count = pd.read_sql("SELECT COUNT(*) as count FROM dataconsolidee", engine)
        print(f"📊 Données dans la view dataconsolidee: {dataconsolidee_count.iloc[0]['count']}")
        

        
        
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")




def run_sample_queries():
    """
    Fait tourner des requêtes SQL intéressantes pour explorer les données 
    """
    print("📈 Running sample analysis queries...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Données sur le nombre d'hôtels en fonction des villes
        sample_villes = pd.read_sql("SELECT c.ville_principale, d.capacity_hotel " \
                                "FROM dataconsolidee d " \
                                "JOIN clusters c ON d.code_cluster = c.code_cluster " \
                                "WHERE d.mois = '01' " \
                                "LIMIT 5", engine)
        print("\n📋 Sample town capacity hotels :")
        print(sample_villes.to_string(index=False))

        # Données météo sur la ville de Toulouse
        sample_toulouse = pd.read_sql("SELECT c.ville_principale, d.température, d.mois " \
                                "FROM dataconsolidee d " \
                                "JOIN clusters c ON d.code_cluster = c.code_cluster " \
                                "WHERE c.ville_principale = 'Toulouse' ", engine)
        print("\n📋 Sample températures Toulouse :")
        print(sample_toulouse.to_string(index=False))

        # Activités à Paris 
        sample_parcs = pd.read_sql("SELECT c.ville_principale, d.nom_du_poi " \
                                "FROM datatourisme d " \
                                "JOIN clusters c ON d.code_cluster = c.code_cluster " \
                                "WHERE c.ville_principale = 'Paris' AND d.categorie_simplifiee = 'parcs' " \
                                "LIMIT 10", engine)
        print("\n📋 Sample parks Paris :")
        print(sample_parcs.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error running sample queries: {e}")

#

if __name__ == "__main__":
    """Test the loading functions"""
    print("Testing database loading functions...\n")
    
    # Test database connection first
    if test_database_connection():
        print("\nDatabase connection OK. Ready for data loading!")

        sample_clusters = pd.read_csv('data/data_transformed/cluster_mapping.csv')
        sample_meteo = pd.read_csv('data/data_transformed/cluster_meteo.csv')
        sample_affluence = pd.read_csv('data/data_transformed/cluster_affluence.csv')
        sample_datatourism = pd.read_csv('data/data_transformed/datatourism_cleaned.csv')
        sample_scoretourism = pd.read_csv('data/data_transformed/datatourism_score_cluster.csv')
        
        
        
        # Test loading 
        print('========== LOADING CLUSTERS ========== ')
        load_to_database_clusters(sample_clusters)

        print('========== LOADING METEO ========== ')
        load_to_database_meteo(sample_meteo)

        print('========== LOADING AFFLUENCE ========== ')
        load_to_database_affluence(sample_affluence)

        print('========== LOADING DATATOURISM ========== ')
        load_to_database_datatourism(sample_datatourism, sample_scoretourism)
        
        print('========== CREATING DATACONSOLIDEE VIEW ========== ')
        create_view()

        print('\n')
        print('========== VERIFYING DATA LOADED IN DATABASE ========== ')
        verify_data()

        print('\n')
        print('========== RUNNING SAMPLE QUERIES ========== ')
        run_sample_queries()
        

    else:
        print("Fix database connection before testing loading functions")
