import pandas as pd
import requests

class DataTourismExtractor():
    def __init__(self, list_chemin) -> None:
        self.list_chemin = list_chemin

    def extract_csv(self):
        """Fonction d'appel à l'API du site du gouvernement afin de telecharger les fichier CSV de DataTourisme sur chaque région 
        et de les stocker"""

        api_url = "https://www.data.gouv.fr/api/2/datasets/5b598be088ee387c0c353714/resources/?page=1&page_size=50"

        
        try:
            print("Request à l'API")
            


            response = requests.get(api_url, timeout=10)
            


            if response.status_code == 200: 
                print("Cela fonctionne")



                data = response.json() 
            
                for part in data.get('data', []):
                    # On récupère les titres des fichier 
                    title = part.get('title')
                    
                    # Si on est dans la liste des csv a garder
                    if title in  self.list_chemin:
                        
                        url_csv  = part.get('url')

                        # on fait un request à l'api du csv en question
                        requests_csv = requests.get(url_csv)

                        if requests_csv.status_code == 200:
                            
                            # On telecharge sous format CSV le fichier
                            with open("../data/DataTourism/"+ title, "wb") as f:
                                f.write(requests_csv.content)


            print(f"Tout les CSV sont créer")
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error fetching data: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error processing data: {e}")
            return pd.DataFrame()
    

    def extract_data(self):
        """Fonctionqui récupère chaque dataframe de chaque région et les concatène dans un seul dataframe"""

        
        try:

            df = pd.DataFrame()

            # Concaténation des dataframe
            for chemin in self.list_chemin:
                df = pd.concat([df, pd.read_csv("../data/DataTourism/"+ chemin)], ignore_index=True)

            print('Dataframe créer')
            return df
            
        except Exception as e:
            print(f"❌ Error reading data: {e}")
            return pd.DataFrame()
        


if __name__ == "__main__":
    list_df = ["datatourisme-reg-ara.csv", "datatourisme-reg-bfc.csv", "datatourisme-reg-bre.csv",
        "datatourisme-reg-cor.csv", "datatourisme-reg-cvl.csv", "datatourisme-reg-gde.csv",
        "datatourisme-reg-hdf.csv", "datatourisme-reg-naq.csv", "datatourisme-reg-nor.csv",
        "datatourisme-reg-idf.csv",  "datatourisme-reg-occ.csv", "datatourisme-reg-pac.csv",
        "datatourisme-reg-pdl.csv"]
    
    extractor = DataTourismExtractor(list_df)
    
    # extractor.extract_csv()
    df = extractor.extract_data()

    print(df)
