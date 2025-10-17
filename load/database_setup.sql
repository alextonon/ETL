-- Setup Database Voyage Voyage
-- Run le fichier une fois qu'on est sûr (et vérifier avec quel compte les tables sont créées)

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS communes;
DROP TABLE IF EXISTS tourisme;
DROP TABLE IF EXISTS meteo;
DROP TABLE IF EXISTS affluences;

-- Table communes : contient les informations relatives à chaque commune française
" CREATE TABLE communes (
    id SERIAL PRIMARY KEY,
    code_insee VARCHAR(5),
    nom_standard VARCHAR(100),
    reg_code INT,
    reg_nom VARCHAR(100),
    dep_code VARCHAR(2),
    dep_nom VARCHAR(100),
    epci_code INT,
    epci_nom VARCHAR(100),
    code_postal INT,
    zone_emploi INT,
    code_insee_centre_zone_emploi VARCHAR(5),
    nb_habitants INT,
    superficie_km2 INT,
    densite FLOAT,
    latitude_mairie FLOAT,
    longitude_mairie FLOAT,
    grille_densite INT,
    grille_densite_texte VARCHAR(100)
);" 

CREATE TABLE communes (
    code_cluster VARCHAR(5) PRIMARY KEY,
    code_insee_centre_zone_emploi,nom_standard,latitude_centre,longitude_centre

)

-- Table tourisme : contient les informations relatives aux points d'intérêt du territoire français
CREATE TABLE tourisme (
    id SERIAL PRIMARY KEY
);

-- Table affluences : contient les données relatives à l'affluence touristique de chaque zone d'emploi du territoire français
CREATE TABLE affluences (
    id SERIAL PRIMARY KEY,
    zone_emploi INT,
    id_activity VARCHAR(10),
    activity_type VARCHAR(100),
    time_period VARCHAR(10),
    capacity_zone FLOAT,
    nb_nights_zone FLOAT
)

-- Table meteo : contient les données relatives à la météo de chaque zone d'emploi du territoire français
CREATE TABLE meteo (
    code_cluster INT,
    Mois INT(2)
    Pression station FLOAT
    Température (°C) FLOAT
    Précipitations dans les 24 dernières heures FLOAT
    Rafales sur une période FLOAT
    PRIMARY KEY (code_cluster, mois)
)



-- Create indexes for better query performance
CREATE INDEX idx_code_insee ON communes(code_insee);
CREATE INDEX idx_region ON communes(reg_code);
CREATE INDEX idx_departement ON communes(dep_code);
CREATE INDEX idx_epci_code ON communes(epci_code);
CREATE INDEX idx_zone_emploi ON affluences(zone_emploi);

-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;
