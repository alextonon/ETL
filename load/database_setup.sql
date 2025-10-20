-- Setup Database Voyage Voyage
-- Run le fichier une fois qu'on est sûr (et vérifier avec quel compte les tables sont créées)

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS communes;
DROP TABLE IF EXISTS tourisme;
DROP TABLE IF EXISTS meteo;
DROP TABLE IF EXISTS affluences;

-- Table communes : contient les informations relatives à chaque commune française


-- Table tourisme : contient les informations relatives aux points d'intérêt du territoire français
--CREATE TABLE tourisme (
   -- id SERIAL PRIMARY KEY
--);

-- Table affluences : contient les données relatives à l'affluence touristique de chaque zone d'emploi du territoire français


-- Table meteo : contient les données relatives à la météo de chaque zone d'emploi du territoire français




-- Create indexes for better query performance
CREATE INDEX idx_code_insee ON communes(code_insee);
CREATE INDEX idx_region ON communes(reg_code);
CREATE INDEX idx_departement ON communes(dep_code);
CREATE INDEX idx_epci_code ON communes(epci_code);
CREATE INDEX idx_zone_emploi ON affluences(zone_emploi);

-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;
