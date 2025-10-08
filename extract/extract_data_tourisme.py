import pandas as pd
import requests

def extract_csv(list_csv):


    api_url = "https://www.data.gouv.fr/api/2/datasets/5b598be088ee387c0c353714/resources/?page=1&page_size=50" # La requette est sur le /states/all et c'est la que les param sont important et préciser
    

    
    try:
        print("Making API request... (this may take a few seconds)")
        


        response = requests.get(api_url, timeout=10)
        


        if response.status_code == 200: #status_code = le statu html de la reponse et 200 c'est quand c'est bon
            print("Cela fonctionne")



            data = response.json() # Vas cherche les données de la request (voir ca comme les ROS)
        
            for part in data.get('data', []):

                title = part.get('title')
                

                if title in  list_csv:
                    
                    url_csv  = part.get('url')

                    requests_csv = requests.get(url_csv)

                    if requests_csv.status_code == 200:

                        with open("data/"+ title, "wb") as f:
                            f.write(requests_csv.content)


        print(f"Created all CSV")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return pd.DataFrame()
    

def extract_data(list_chemin):

    # print("📄 Reading tourism data from CSV...")
    
    try:

        df = pd.DataFrame()
        for chemin in list_chemin:
            df = pd.concat([df, pd.read_csv("../data/"+ chemin)], ignore_index=True)

        print('Data retreived in a dataframe')
        return df
        
    except Exception as e:
        print(f"❌ Error reading data: {e}")
        return pd.DataFrame()
    

list_df = ["datatourisme-reg-ara.csv", "datatourisme-reg-bfc.csv", "datatourisme-reg-bre.csv",
        "datatourisme-reg-cor.csv", "datatourisme-reg-cvl.csv", "datatourisme-reg-gde.csv",
        "datatourisme-reg-hdf.csv", "datatourisme-reg-naq.csv", "datatourisme-reg-nor.csv",
        "datatourisme-reg-idf.csv",  "datatourisme-reg-occ.csv", "datatourisme-reg-pac.csv",
        "datatourisme-reg-pdl.csv"]

# extract_csv(list_df)

df = extract_data(list_df)