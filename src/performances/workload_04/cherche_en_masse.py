#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
from py2neo import Graph
import pyorient
import random
import re
import sys
import threading
import time

from common import affiche_temps

class Entite(object):
    '''Simule une response neo4J sous la forme { 'id':'identifiant de l objet', 'node':'l objet' }'''
    def __init__(self,_id,_node):
        self.id = _id
        self.node = _node


class Cherche(object):
    ''' '''
    def __init__(self,):
        self.recherches = {
            'AMISAMISAMISAMISPLUSJEUNEDESEXEDIFF': 'Ses amis et les amis de ses amis jusqu\'au 4e niveau de sexe different et du meme age ou plus jeune',
            'MEMENUMEROPLUSAGES': 'Personnes plus agées habitant le meme numero que lui',
            'MEMERUEMEILLEURSALAIRE': 'Personnes de sa rue qui ont un meilleur salaire que lui, trié par les plus hauts salaires',
            'MEMETRAVAILPLUSJEUNEMEMESEXEAUTREVILLEMEILLEURSALAIRE': 'Personnes travaillant dans le meme domain, plus jeune et ayant un salaire plus elevé de meme sexe dans une autre ville, trié par les plus hauts salaires',
            'LIENPLUSCOURT2PERSONNES': 'Lien social le plus court entre 2 personnes (sur 10 niveaux maximum)',
            # Algo pas disponible pour OrientDB donc pas de comparatif possible
        #    'TOUSLIENSPLUSCOURTS2PERSONNESMAX4NIVEAU':'Tous les liens socials plus court entre 2 personnes sur 4 niveaux maximum',
            'SUMSALAIREFAMILLEADULTES': 'La somme des salaires de la famille des plus de 20 ans vivant dans le meme logement',
        }
        
        if self.nb_personnes == 0 or self.nb_habitations == 0:
            print("La base ne doit pas etre vide d'habitants (Personne habitant dans un Numero")
            print("Remplir la base avec les workload precedents")
            exit(1)
        
        if not os.path.isfile(self.filename) :
            self.ecrit("STATUT;REQUETE;DUREE(MS);DUREE(S MS);NB_TROUVE;NB_NOEUDS;NB_ARETES")
        
    def ecrit(self, _ligne):
        f = open(self.filename,'a')
        f.write("%s\n" % _ligne)
        f.close()
        print _ligne
        
    def cherche(self, _num_cli):
        #for recherche,label in self.recherches.iteritems():
        desordre = self.recherches.items()
        random.shuffle(desordre)
        for recherche,label in desordre:
            larequete = self.requetes[recherche]
            print("  EXECUTE%s --> %s" % (_num_cli, larequete) )
            t1=datetime.datetime.now()
            try:
                qte_resultat = self.execute_une_requete( larequete )
                statut = "OK"
            except Exception as e:
                print("WARNING;%s;  Erreur lors de l'execution de la requete.")
                print("%s" % (e) )
                statut = "KO"
                qte_resultat = -1
            
            t2=datetime.datetime.now()
            print("  DUREE%s --> %s" % (_num_cli,affiche_temps(recherche, t2-t1)) )
            
            self.ecrit( "%s;%s;%s;%s;%s" % (statut,affiche_temps(recherche, t2-t1), qte_resultat, self.nb_noeuds, self.nb_aretes) )


class ChercheOrientDB(Cherche):
    ''' '''
    def __init__(self,):
        self.client = pyorient.OrientDB(os.environ['ORIENTDBSERV'], int(os.environ['ORIENTDBPORT']))
        self.session_id = self.client.connect( os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        self.client.db_open( os.environ['ORIENTDBBASE'], os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        
        self.nb_noeuds = self.client.command( "select count(*) from V")[0].count
        self.nb_aretes = self.client.command( "select count(*) from E")[0].count
        self.clusterpersonneid = self.client.command( "select from Personne limit 1")[0]._rid.split(':')[0]
        self.clusterhabitatid = self.client.command( "select from Numero limit 1")[0]._rid.split(':')[0]
        self.nb_personnes = self.client.data_cluster_count([int(self.clusterpersonneid[1:])])
        self.nb_habitations = self.client.data_cluster_count([int(self.clusterhabitatid[1:])])
        self.last_personne = Entite("#%s:0" % self.clusterpersonneid, {})
        self.last_Habitation = Entite("#%s:0" % self.clusterhabitatid, {})
        
        self.filename = "%s/workload_04/compteurs.workload_04.orientdb.csv" % os.environ['BENCH_HOME']
        super(ChercheOrientDB, self).__init__()
        
        
    def mk_requests(self,):
        ''' Genere des requetes avec des elements au hasard suivant les patterns'''
        print self.get_random_personne()
        print( "TESTDEVARIABLES => %s %s %s" % (self.get_random_personne().id, self.last_personne.node.sexe, self.last_personne.node.age) )
        self.requetes = { 
            'AMISAMISAMISAMISPLUSJEUNEDESEXEDIFF': 'select from (SELECT expand( set(both("AMI").both("AMI").both("AMI").both("AMI")) )  FROM %s ) where sexe <> "%s" and %s >= age' % (self.get_random_personne().id, self.last_personne.node.sexe, self.last_personne.node.age), 
            'MEMENUMEROPLUSAGES': 'select from Personne where out("HABITE").no in (select out("HABITE").no from %s) and age >= %s' % (self.get_random_personne().id, self.last_personne.node.age), 
            'MEMERUEMEILLEURSALAIRE': 'select from Personne where out("HABITE").in("COMPORTE") in (select out("HABITE").in("COMPORTE") from %s) and salaire > %s order by salaire desc' % (self.get_random_personne().id, self.last_personne.node.salaire), 
            'MEMETRAVAILPLUSJEUNEMEMESEXEAUTREVILLEMEILLEURSALAIRE': 'select *, out("HABITE").in("COMPORTE").in("ORGANISE").libcom from Personne where out("HABITE").in("COMPORTE").in("ORGANISE") not in (select out("HABITE").in("COMPORTE").in("ORGANISE") from %s) and activite in (select activite from %s) and sexe in (select sexe from %s) and age <> %s and salaire > %s order by salaire desc' % (self.get_random_personne().id, self.last_personne.id, self.last_personne.id, self.last_personne.node.age, self.last_personne.node.salaire), 
            'LIENPLUSCOURT2PERSONNES': 'select shortestPath(%s, %s, "BOTH")' % (self.get_random_personne().id, self.get_random_personne().id), 
        #    'TOUSLIENSPLUSCOURTS2PERSONNESMAX4NIVEAU': 'ALGO PAS DISPONIBLE sur OrientDB' % (self.get_random_personne().id, self.get_random_personne().id), 
            'SUMSALAIREFAMILLEADULTES': 'select max(salaire) from Personne where out("HABITE") in (select out("HABITE") from %s) and nom = "%s"'  % (self.get_random_personne().id, self.last_personne.node.nom),
        }
    
    def execute_une_requete(self, _req):
        ''' execute une recherche'''
        r = self.client.command(_req)
        #print "   RESULTAT => %d entrees" % len(r)
        return len(r)
    
    def get_random_habitation(self,):
        '''Retourne une habitation(Numero) pas utilise ni teste pb si on supprime des Numero !'''
        p =[]
        while len(p) == 0: habitat = self.client.command( "select from %s:%s" %(self.clusterhabitatid, random.randint(0,self.nb_habitations) ) )
        self.last_habitation = Entite(habitat[0]._rid, habitat[0])
        return Entite(habitat[0]._rid, habitat[0])
            
    def get_random_personne(self,):
        '''Retourne une personne deja crée au hasard'''
        p =[]
        while len(p) == 0: p = self.client.command( "select from %s:%s" % (self.clusterpersonneid, random.randint(0,self.nb_personnes)) )
        self.last_personne = Entite(p[0]._rid, p[0])
        return Entite(p[0]._rid, p[0])
    
    def close(self):
        self.client.db_close()

class ChercheNeo4J(Cherche):
    ''' '''
    def __init__(self,):
        self.graph = Graph(os.environ["NEO4JDBSTR"])
        self.nb_habitations = self.graph.cypher.execute("match (n:Numero) return count(n) as count")[0].count
        self.nb_personnes = self.graph.cypher.execute("match (p:Personne) return count(p) as count")[0].count
        
        self.nb_noeuds = self.graph.cypher.execute( "match (n) return count(n) as count")[0].count
        self.nb_aretes = self.graph.cypher.execute( "match ()-[r]->() return count(r) as count")[0].count
        
        self.filename = "%s/workload_04/compteurs.workload_04.neo4j.csv" % os.environ['BENCH_HOME']
        super(ChercheNeo4J, self).__init__()
    
    def mk_requests(self,):
        ''' Genere des requetes avec des elements au hasard suivant les patterns'''
        self.requetes = { 
            'AMISAMISAMISAMISPLUSJEUNEDESEXEDIFF': 
                'match (n:Personne)-[:AMI*..4]-(m:Personne) where id(n) = %d and n.age >= m.age and n.sexe <> m.sexe return n,m' % (self.get_random_personne().id), 
            'MEMENUMEROPLUSAGES': 
            'match (p1:Personne)-[:HABITE]-(r1:Numero) ,(p2:Personne)-[:HABITE]-(r2:Numero) where id(p1) = %d and p1.age < p2.age and id(p1)<>id(p2) and r1.no = r2.no return p1,r1,p2,r2' % (self.get_random_personne().id), 
            'MEMERUEMEILLEURSALAIRE': 'match (p:Personne)-[:HABITE]->(n:Numero)<-[:COMPORTE]-(v:Voie)-[:COMPORTE]->(nn:Numero)<-[:HABITE]-(pp:Personne) where id(p) = %d and p.salaire < pp.salaire return p,n,v,nn,pp ORDER BY pp.salaire DESC' % (self.get_random_personne().id), 
            'MEMETRAVAILPLUSJEUNEMEMESEXEAUTREVILLEMEILLEURSALAIRE': 'start p=node(%d) match (p)-[:HABITE]->(n:Numero)<-[:COMPORTE]-(v:Voie)<-[:ORGANISE]-(c:Commune),(pp:Personne)-[:HABITE]->(nn:Numero)<-[:COMPORTE]-(vv:Voie)<-[:ORGANISE]-(cc:Commune) where p.sexe = pp.sexe and p.activite = pp.activite and c.libcom <> cc.libcom  return p,pp,v,vv,c,cc order by pp.salaire DESC' % (self.get_random_personne().id),
        # Pour les requetes avec allShortestPaths et shortestPath on ne peut pas retourner p a cause d'un bug (https://github.com/nigelsmall/py2neo/issues/400) mais p, donc le chemin, est quand meme calculer donc ca nous va pour la mesure.
            'LIENPLUSCOURT2PERSONNES':'start p1=node(%d) , p2=node(%d) match p=shortestPath((p1)-[*..10]-(p2)) return p1' % (self.get_random_personne().id, self.get_random_personne().id),
            'TOUSLIENSPLUSCOURTS2PERSONNESMAX4NIVEAU':'start p1=node(%d) , p2=node(%d) match p=allShortestPaths((p1)-[*..4]-(p2)) return p1' % (self.get_random_personne().id, self.get_random_personne().id),
            'SUMSALAIREFAMILLEADULTES':'start p=node(%d) match (n:Numero)<-[:HABITE]-(p)-[:FAMILLE]-(pp:Personne)-[:HABITE]->(nn:Numero) where n = nn and toInt(pp.age) > 20 return sum(toInt(pp.salaire))+toInt(p.salaire)' % (self.get_random_personne().id),
        }
    
    def execute_une_requete(self, _req):
        ''' execute une recherche'''
        #req = self.graph.cypher.execute(_req)
        ## Passage par une transaction pour pouvoir utiliser les functions shortpath etc.
        tx = self.graph.cypher.begin()
        tx.append(_req)
        tx.process()
    
    def get_random_habitation(self,):
        '''Retourne une habitation(Numero) vide (le seul lien qu'elle a est le lien du Numero à sa rue) en random'''
        habitat, = self.graph.cypher.execute( "match (n:Numero) where not ()-[:HABITE]->(n) return id(n) as id,n as node SKIP %s LIMIT 1" %( random.randint(0,self.nb_habitations) ) )
        return habitat
    
    def get_random_personne(self,):
        '''Retourne une personne deja crée au hasard
        Parfois de facon sporadique on a un resultat vide de la requete suivante !!'''
        p =[]
        while len(p) == 0: p = self.graph.cypher.execute( "match (n:Personne) return id(n) as id,n as node SKIP %s LIMIT 1" %( random.randint(0,self.nb_personnes) ) )
        return p[0]
    
    
    def close(self):
        pass

def usage():
    print("usage: %s orientdb | neo4j" % sys.argv[0] )
    exit(1)

def executer(_occurence, _cherche):
    _cherche.mk_requests()
    t1=datetime.datetime.now()
    _cherche.cherche(_occurence)
    _cherche.close()
    print affiche_temps("FINCLIENT%s" % _occurence, datetime.datetime.now() - t1)

if __name__ == "__main__":
    ## Nombre de sequences de requetes
    NB_BOUCLE = 1000
    
    ## Nombre de clients concurrents qui vont executer les requetes
    NB_CONCURRENT = 5
    
    if len(sys.argv) != 2 or not (sys.argv[1] in ("orientdb","neo4j")) :
        usage()
    
    ## Par thread boucle
    for thread_num in range(0,NB_BOUCLE):
        for thread_num in range(0,NB_CONCURRENT):
            print ("DEBUTCLIENT%s" % thread_num)
            if sys.argv[1] == "orientdb":
                cherche = ChercheOrientDB()
            elif sys.argv[1] == "neo4j":
                cherche = ChercheNeo4J()
            try:
                thread = threading.Thread(target = executer, args = (thread_num,cherche,))
                thread.start()
            except:
                print "Probleme lancement du thread client."
        # On attend
        # <_MainThread(MainThread, started 139670512949056)> reste dans la pile. bizarre ?
        while threading.activeCount() > 1:
            print "Threads en cours : %s" % (threading.activeCount()-1)
            time.sleep(3)
    
    '''
    ## Par boucle non concurrente si pas plusieurs coeurs
    if sys.argv[1] == "orientdb":
        cherche = ChercheOrientDB()
    elif sys.argv[1] == "neo4j":
        cherche = ChercheNeo4J()
    
    for thread_num in range(0,NB_BOUCLE):
        executer(thread_num,cherche)
    cherche.close()
    '''
