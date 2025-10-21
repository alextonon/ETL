import pandas as pd

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

if __name__ == "__main__":
    town_extractor = TownExtractor()
    df_town = town_extractor.extract_data(["communes-france-2025.csv"])

    print(df_town.head()) 

    df_town.to_csv("data/data_extracted/df_town.csv", index=False)