from load.load_data import get_connection_string
from sqlalchemy import create_engine
import pandas as pd

def compute_score (preference, month):
    """
    Fonction qui permet de calculer le score de chaque cluster en fonction des préférences de l'utilisateur
    et qui renvoie un  dataframe trier des clusters
    
    Args:
        preference (dict) : dictionnaire des poids pour chaque catégorie permettant de fiter les préférence de l'utilisateur
        month (int) : numéro du mois de la recherche
    Return:
        df_score (Dataframe) : dataframe des score des cluster triée par ordre décroissant
    """
    print("Calcule du score  des cluster.")
    
    connection_string = get_connection_string()
    
    try:

        # Conection à notre database
        engine = create_engine(connection_string)

        # On récupère la view des scores
        df_score  = pd.read_sql("SELECT * FROM dataconsolidee", engine)

        # On ne garde que les données du mois qui nous intéresse
        df_score = df_score[df_score['mois'] == month]

        df_score["affluence_touristique"] = df_score["nb_nights_camping"] + df_score["nb_nights_hotel"]/ (df_score["capacity_camping"] + df_score["capacity_hotel"] + 1)

        # On parcours chaque cluster et on calcule le score final en multipliant les poids au score des catégorie et en sommant
        score_list = []
        for i in range(len(df_score)):
            score_temp = 0
            for j, categorie in enumerate(df_score.columns):
                if categorie in preference.keys():
                    score_temp += df_score.iloc[i, j] * preference[categorie]

            score_list.append(score_temp)

        # On ajoute la colonne des scores et on ne garde que celle la et les codes clusters
        df_score['Score'] = score_list

        cols = ['code_cluster', 'Score']

        df_score = df_score[cols]

        # On trie par ordre décroissant des scores
        df_score = df_score.sort_values(by=['Score'], ascending=[False]).reset_index(drop=True)

        engine.dispose()

        return df_score
        
    except Exception as e:
        print(f"❌ Error computing score: {e}")


if __name__ == "__main__":
    # Ici, on simule les choix d'un utilisateur. On voit que l'utilisateur préfère les températures élevées (poids température = 2),
    # mais qu'il ne veut absolument pas de pluie (précipitations_24_dernières_heures = -1,5). Il préfère qu'il y ait une grande capacité d'hôtels / campings,
    # ce qui suggère un côté plus urbain. Pour les activités, on constate que l'utilisateur ne veut pas faire de sport,
    # car les poids ne sont qu'à 1,0 pour ce type d'activité ; les visites semblent davantage être son fort, car les poids sont à 2,0.
    # Certains poids sont cachés et resteront toujours à 1 : score_centres_de_tourisme, score_logements, score_magasins,
    # score_nourriture, score_transports, car ce sont des catégories qui montrent qu'il y a des infrastructures touristiques
    # mais qui ne sont pas des préférences. Enfin, comme nous sommes en été, le poids des sports d'hiver est à 0.
    poids_preference = {
        'pression_station': 1.0, 'température': 2, 'précipitations_24_dernières_heures': -1.5,
        "affluence_touristique": -1.0,
        'score_activités': 1.0, 'score_balades': 1.0, 'score_centres_de_tourisme': 1.0,
        'score_culture': 2.0, 'score_evenements': 2.0, 'score_logements': 1.0,
        'score_magasins': 1.0, 'score_nourriture': 1.0, 'score_parcs': 2.0, 'score_sorties_soir': 2.0,
        'score_sports': 1.0, 'score_sports_hiver': 0, 'score_transports': 1.0
    }

    df_exemple = compute_score(poids_preference, 7)

    print(df_exemple.head(10))
