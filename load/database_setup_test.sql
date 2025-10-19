-- Setup Database Voyage Voyage
-- Run le fichier une fois qu'on est sûr (et vérifier avec quel compte les tables sont créées)

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS meteo;
DROP TABLE IF EXISTS affluence;
-- Table communes : contient les informations relatives à chaque commune française



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
    Mois INT,
    Pression_station FLOAT,
    Température FLOAT,
    Précipitations_24_dernières_heures FLOAT,
    Rafales_sur_une_période FLOAT,
    PRIMARY KEY (code_cluster, mois)
);





-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;
