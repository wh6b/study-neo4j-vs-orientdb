CREATE INDEX ON :Numero (lib_off);
CREATE INDEX ON :Numero (no);
CREATE INDEX ON :Numero (vide);

USING PERIODIC COMMIT 50
LOAD CSV WITH HEADERS FROM "file:/home/nicov/Work/ENG221/data/Numeros.clean.csv" AS line FIELDTERMINATOR ';'
MATCH (voie:Voie { libelle:line.lib_off , commune:'TOULOUSE' } )

CREATE (numero:Numero { lib_off:line.lib_off, no:line.no, sti:line.sti, rivoli:line.rivoli, X_WGS84:line.X_WGS84, Y_WGS84:line.Y_WGS84, vide:line.true })
CREATE (voie)-[:COMPORTE]->(numero);

//Exemple : tous les numeros de la rue de chaussas
//match (n:Numero)-[r]-(v:Voie {libelle:"RUE DE CHAUSSAS"})-[s]-(ville) return n,r,m,s,ville limit 200
