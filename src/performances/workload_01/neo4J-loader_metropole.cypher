
 "columns": [ "code_insee:Integer" , "libelle:string" , "libcom:string" , "cugt:Boolean" , "code_fantoir:Integer"]

LOAD CSV WITH HEADERS FROM "file:/home/nicov/Work/ENG221/data/Limites_Communes.csv" AS line
MERGE (commune:Commune { code_insee: toInt(line.code_insee), libelle:line.libelle, libcom:line.libcom, cugt:line.cugt, code_fantoir:toInt(line.code_fantoir) })




CREATE (ue:UE { code: line.UE, cursus: line.cursus })
CREATE (commune)-[:VALIDE { note:toInt(line.note), annee:toInt(line.annee)}]->(ue)
