-- Setup Database Voyage Voyage
-- Run le fichier une fois qu'on est sûr (et vérifier avec quel compte les tables sont créées)

-- Drop tables if they exist (for clean restart)
DROP VIEW IF EXISTS dataconsolidee;

DROP TABLE IF EXISTS meteo;
DROP TABLE IF EXISTS affluence;
DROP TABLE IF EXISTS clusters;
DROP TABLE IF EXISTS datatourisme;
DROP TABLE IF EXISTS scoretourisme;


-- Table clusters : contient les informations relatives à chaque cluster 

CREATE TABLE clusters (
    code_cluster INT PRIMARY KEY,
    code_insee_centre_zone_emploi VARCHAR(5),
    latitude_centre FLOAT,
    longitude_centre FLOAT,
    ville_principale TEXT
);



-- Table affluences : contient les données relatives à l'affluence touristique de chaque zone d'emploi du territoire français
CREATE TABLE affluence (
    code_cluster INT,
    mois INT,
    nb_nights_camping FLOAT,
    nb_nights_hotel FLOAT,
    capacity_camping FLOAT,
    capacity_hotel FLOAT,
    PRIMARY KEY (code_cluster, mois)
);

-- Table meteo : contient les données relatives à la météo de chaque zone d'emploi du territoire français
CREATE TABLE meteo (
    code_cluster INT,
    mois INT,
    pression_station FLOAT,
    température FLOAT,
    précipitations_24_dernières_heures FLOAT,
    rafales_sur_une_période FLOAT,
    PRIMARY KEY (code_cluster, mois)
);


-- Table tourisme : contient les informations relatives aux points d'intérêt du territoire français
CREATE TABLE datatourisme (
   id TEXT,
   code_cluster INT,
   nom_du_poi TEXT,
   categories_de_poi TEXT,
   categorie_simplifiee TEXT,
   latitude FLOAT,
   longitude FLOAT,
   date_de_mise_a_jour DATE,
   description TEXT,
   uri_id_du_poi TEXT,
   PRIMARY KEY (id)
);

CREATE TABLE scoretourisme (
    code_cluster INT,
    activités INT,
    balades INT,
    centres_de_tourisme INT,
    culture INT,
    evenements INT,
    logements INT,
    magasins INT,
    nourriture INT,
    parcs INT,
    sorties_soir INT,
    sports INT,
    sports_hiver INT,
    transports INT,
    PRIMARY KEY (code_cluster)
);


-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;

