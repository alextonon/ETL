import io
import pandas as pd
import requests
import os

class DataTourismExtractor():
    def __init__(self) -> None:
        self.list_chemin = ["datatourisme-reg-ara.csv", "datatourisme-reg-bfc.csv", "datatourisme-reg-bre.csv",
        "datatourisme-reg-cor.csv", "datatourisme-reg-cvl.csv", "datatourisme-reg-gde.csv",
        "datatourisme-reg-hdf.csv", "datatourisme-reg-naq.csv", "datatourisme-reg-nor.csv",
        "datatourisme-reg-idf.csv",  "datatourisme-reg-occ.csv", "datatourisme-reg-pac.csv",
        "datatourisme-reg-pdl.csv"]


    def extract_data(self):
        """Fonction d'appel à l'API du site du gouvernement afin de telecharger les fichiers CSV de DataTourisme sur chaque région 
        et de les stocker
        Args:
            None
        Returns:
            df (pd.DataFrame) : DataFrame contenant les informations relatives à tous les POI de France.
        """

        api_url = "https://www.data.gouv.fr/api/2/datasets/5b598be088ee387c0c353714/resources/?page=1&page_size=50"

        
        try:
            print("Request à l'API")
            


            response = requests.get(api_url, timeout=10)
            


            if response.status_code == 200: 
                print("Cela fonctionne")

                data = response.json() 

                df = pd.DataFrame()
            
                for part in data.get('data', []):

                    # On récupère les titres des fichiers 
                    title = part.get('title')
                    
                    # Si on est dans la liste des csv à garder
                    if title in  self.list_chemin:
                        
                        url_csv  = part.get('url')

                        # on fait un request à l'api du csv en question
                        requests_csv = requests.get(url_csv)

                        if requests_csv.status_code == 200:
                            csv_text = requests_csv.content.decode('utf-8')
                            df_csv = pd.read_csv(io.StringIO(csv_text))

                            df = pd.concat([df, df_csv], ignore_index=True)

            print(f"Tous les CSV sont créés")
            return df
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error fetching data: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error processing data: {e}")
            return pd.DataFrame()
    

if __name__ == '__main__' :
    DataTourism_Extractor = DataTourismExtractor()
    df_tourism = DataTourism_Extractor.extract_data()

    print('### --- DONNEES EXTRAITES DATATOURISME --- ###\n')
    print(df_tourism.head())
    
    df_tourism.to_csv("data/data_extracted/df_datatourisme.csv", index=False)

    
