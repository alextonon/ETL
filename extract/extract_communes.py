import pandas as pd



### il faudrait faire la fonction d'appel vers l'URL ?

class TownExtractor():
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
        

    