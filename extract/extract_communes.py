import pandas as pd



### il faudrait faire la fonction d'appel vers l'URL ?

class TownExtractor():
    def extract_data(self, list_chemin):

        print("--- Reading communes data from csv...")

        try:

            df = pd.DataFrame()
            for chemin in list_chemin:
                df = pd.concat([df, pd.read_csv("data/"+ chemin , low_memory=False)], ignore_index=True)

            return df

        except Exception as e:
            print(f"--- ❌ Error reading data: {e}")
            return pd.DataFrame()
        

    