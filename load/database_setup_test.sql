-- Setup Database Voyage Voyage
-- Run le fichier une fois qu'on est sûr (et vérifier avec quel compte les tables sont créées)

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS meteo;
DROP TABLE IF EXISTS affluence;
DROP TABLE IF EXISTS clusters;

-- Table communes : contient les informations relatives à chaque commune française

CREATE TABLE clusters (
    code_cluster VARCHAR(5) PRIMARY KEY,
    code_insee_centre_zone_emploi VARCHAR(5),
    nom_standard VARCHAR(100),
    latitude_centre FLOAT,
    longitude_centre FLOAT,
    ville_principale TEXT
);


-- Table tourisme : contient les informations relatives aux points d'intérêt du territoire français
--CREATE TABLE tourisme (
   -- id SERIAL PRIMARY KEY
--);

-- Table affluences : contient les données relatives à l'affluence touristique de chaque zone d'emploi du territoire français
CREATE TABLE affluence (
    code_cluster INT,
    id_activity VARCHAR(10),
    activity_type VARCHAR(100),
    time_period VARCHAR(10),
    capacity_zone FLOAT,
    nb_nights_zone FLOAT,
    PRIMARY KEY (code_cluster, id_activity)
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





-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;
