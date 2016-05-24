CREATE (NFA004:UE { code:'NFA004',label : 'Architecture des machines', cursus : 'L1' })
CREATE (NFA010:UE { code:'NFA010',label : 'Graphes et optimisation', cursus : 'L2' })
CREATE (NFA008:UE { code:'NFA008',label : 'Bases de données', cursus : 'L2' })
CREATE (ENG221:UE { code:'ENG221',label : 'Information et communication pour l\'ingénieur', cursus : 'Ingénieur' })
CREATE (MPY138847:Auditeur { nom:'Nicolas Vergnes' })
CREATE (MPY138847)-[:SUIS { annee : '2015',examen:'06/2015' }]->(ENG221)
CREATE (MPY138847)-[:VALIDE { note : '12.5',annee : '2009' }]->(NFA004)
CREATE (MPY138847)-[:VALIDE { note : '15.5',annee : '2010' }]->(NFA010)
CREATE (MPY138847)-[:VALIDE { note : '13',annee : '2009' }]->(NFA008)







MATCH (n:`Auditeur`),(u:`UE`) RETURN n,u;



MATCH (n)
OPTIONAL MATCH (n)-[r]-()
DELETE n,r