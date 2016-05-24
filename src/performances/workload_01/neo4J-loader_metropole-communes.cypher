CREATE CONSTRAINT ON (c:Commune) ASSERT c.libcom IS UNIQUE;
LOAD CSV WITH HEADERS FROM "file:/home/nicov/Work/ENG221/data/Limites_Communes.csv" AS line FIELDTERMINATOR ';'
CREATE (c:Commune { code_insee: toInt(line.code_insee), libelle:line.libelle, libcom:line.libcom, cugt:line.cugt, code_fantoir:toInt(line.code_fantoir) });
