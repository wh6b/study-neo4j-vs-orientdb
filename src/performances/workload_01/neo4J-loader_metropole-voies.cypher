CREATE INDEX ON :Voie (libelle);
CREATE CONSTRAINT ON (v:Voie) ASSERT v.sti IS UNIQUE;

USING PERIODIC COMMIT 50
LOAD CSV WITH HEADERS FROM "file:/home/nicov/Work/ENG221/data/Voies.clean.csv" AS line FIELDTERMINATOR ';'
MATCH (commune:Commune { libcom:line.commune } )
CREATE (voie:Voie { libelle_complet:line.libelle_complet, libelle:line.libelle, commune:line.commune, code_insee:toInt(line.code_insee), sti:line.sti, rivoli:line.rivoli})
CREATE (commune)-[:ORGANISE]->(voie);
