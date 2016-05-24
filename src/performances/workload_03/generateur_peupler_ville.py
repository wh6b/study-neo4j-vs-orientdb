#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
from py2neo import Graph, Node, Relationship
import pyorient
import random
import re
import sys

from workload_03.personnes import Habitation
from common import affiche_temps

class PeuplerVille(object):
    '''
    Creer des personnes et des familles
    Trouve un logement disponible pour une famille
    Creer des liens sociaux pour une personne :
    - Liens de famille
    - Liens d'amitie
    - Liens de connaissance
    - Liens de travail ? Non consomme trop de lectures pour ce workload
    '''
    def __init__(self, _nb_habitants):
        ## Compteurs des actions sur la base de donnees
        self.compteurs = { 'noeud':{ 'R':0,'W':0}, 'arete':{ 'R':0,'W':0} }
        
        ## Nombre d'habitants a loger
        self.max_habitants = _nb_habitants
        self.nb_habitants = 0
        
        ## Liens sociaux CONNAIT a 100 normalement
        self.liens = { 'CONNAIT':range(10,70),'FAMILLE':range(0,6),'AMI':range(2,10) }
    
    def incr(self, _type, _action, _qte=1):
        ''' Increment le compteur en fonction de l'action'''
        self.compteurs[_type][_action] += _qte
    def compteurs_tostr(self,):
        ''' Affiche les compteurs des actions'''
        print("Compteurs : %s" % (self.compteurs) )
    
    def peupler(self,):
        habitat = Habitation()
        
        while self.nb_habitants < self.max_habitants:
            ## CREER UNE HABITATION
            ## TROUVER UN LOGEMENT VIDE
            logement = self.get_random_habitation()
            self.habitationutilisee(logement)
            #print (" Habitation %s" % ( logement.node ) )
            print (" Habitation %s" % ( logement ) )
            for f in habitat.next():
                #print "  Nouvelle famille:"
                famille = []
                for p in f:
                    ## CREER UNE PERSONNE ET LA LIER A UN HABITAT
                    personne = self.creerHabitant(p)
                    ####Pour orientDB il faut rerecuperer le logement car il a change de version a chaque fois, pour ne pas avoir d erreur par la suite
                    self.habiter(personne, logement)
                    
                    #h = self.habiter(personne, self.get_habitation_byID(logement))
                    #h = self.habiter(personne, logement)
                    famille.append(personne)
                    self.incr('noeud','W')
                    self.incr('arete','W')
                
                ## On cree d abord les membres d'une famille pour pouvoir les relier par la suite.
                ## CREER LES LIENS SOCIAUX avec des personnes qui n'ont pas atteind leur limite sociale.
                for personne in famille:
                    liens_existants = []
                    self.incr('noeud','R')
                    for c in self.get_membres_famille_from_personne(personne):
                        liens_existants.append(str(c))
                        
                    for personne_meme_famille in famille:
                        if personne != personne_meme_famille:
                            self.creerLiensSociaux(personne,personne_meme_famille,'CONNAIT')
                            self.incr('arete','W')
                            if personne_meme_famille not in liens_existants :
                                #print("creer lien de %s vers %s" % (personne,personne_meme_famille))
                                self.creerLiensSociaux(personne,personne_meme_famille,'FAMILLE')
                                self.incr('arete','W')
                    
                    ## Creation des liens sociaux aleatoires:
                    for lien,nb_probable in self.liens.iteritems():
                        ## Combien de liens faire ?
                        for ocurrence in range(0,random.choice(nb_probable)):
                            #struct_date1=datetime.datetime.now()
                            self.creerLiensSociaux(personne, self.get_random_personne(), lien)
                            #struct_date2=datetime.datetime.now()
                            #diff = struct_date2 - struct_date1
                            #print affiche_temps("TOTAL",{"TOTAL":"durée totale"}, diff)
                            self.incr('noeud','R')
                            self.incr('arete','W')
                self.compteurs_tostr()



class PeuplerVilleOrientDB(PeuplerVille):
    def __init__(self, nb_habitants):
        super(PeuplerVilleOrientDB, self).__init__(nb_habitants)
        
        self.client = pyorient.OrientDB(os.environ['ORIENTDBSERV'], int(os.environ['ORIENTDBPORT']))
        self.session_id = self.client.connect( os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        self.client.db_open( os.environ['ORIENTDBBASE'], os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        
        try:
            self.client.command( "create class Personne extends V" )
            self.client.command( "create class HABITE extends E" )
            self.client.command( "create property Personne.prenom string" )
            self.client.command( "create property Personne.nom string" )
            self.client.command( "create property Personne.sexe string" )
            self.client.command( "create property Personne.age string" )
            
            self.client.command( "create index I_prenom ON Personne (prenom) notunique" )
            self.client.command( "create index I_nom ON Personne (nom) notunique" )
            self.client.command( "create index I_sexe ON Personne (sexe) notunique" )
            self.client.command( "create index I_age ON Personne (age) notunique" )
            
        except:
            print( "La classe Personne existe déjà (avec ces index normalement)." )
        self.clusterpersonneid = self.client.command( "create vertex Personne content {'prenom':'TEMP'}")[0]._rid.split(':')[0]
        self.client.command( "delete vertex Personne where prenom = 'TEMP'" )
        self.clusterhabitatid = self.client.command( "select from Numero limit 1")[0]._rid.split(':')[0]
        print( "Cluster ID de la classe Personne est %s. Cluster ID de la classe Numero est %s" % (self.clusterpersonneid, self.clusterhabitatid) )
        
        for  lien in self.liens.keys():
            try:
                self.client.command( "create class %s extends E" % lien)
            except:
                print( "La classe edge %s existe déjà." %lien )
        
        self.nb_habitations = self.client.data_cluster_count([int(self.clusterhabitatid[1:])])
        
    def get_random_habitation(self,):
        '''Retourne une habitation(Numero) vide (le seul lien qu'elle a est le lien du Numero à sa rue)'''
        return self.client.command( "select from Numero where vide = True limit 1" )[0]._rid
        return self.client.command( "select from Numero where vide = True limit 1" )[0]
    
    def get_habitation_byID(self, _habitation):
        return self.client.command( "select from %s" %(_habitation) )[0]
        #return self.client.command( "select from %s" %(_habitation._rid) )[0]
    
    def habitationutilisee(self, _habitation):
        self.client.command( "update %s set vide = false lock record" % _habitation)
        #self.client.command( "update %s set vide = false lock record" % _habitation._rid)
    
    def get_random_personne(self,):
        '''Retourne une personne deja crée au hasard'''
        p = None
        essai = 0
        nb_personnes = self.client.data_cluster_count([int(self.clusterpersonneid[1:])])
        while not p and essai < 100:
            essai +=1
            p = self.client.command( "select from %s:%s" %(self.clusterpersonneid, random.randint(0,nb_personnes) ) )
        if p:
            return p[0]._rid
        else:
            return None
        
    def habiter(self,personne, logement):
        '''Lie une personne(Personne) à un habitat (Numero)'''
        cmd = "create edge HABITE from %s to %s" % (personne, logement)
        
        self.client.command( cmd )
    
    def creerHabitant(self, personne):
        #print( "   Habitant: %s" % personne.to_json() )
        cmd = "create vertex Personne content %s" % ( personne.to_json() )
        req = self.client.command( cmd )
        self.nb_habitants += 1
        return req[0]._rid
        
    def creerLiensSociaux(self, personneRIDA, personneRIDB, type="CONNAIT"):
        #cmd = "create edge %s from %s to (select from Personne where libelle = '%s' and in(ORGANISE)['libcom'] = '%s' )" % (type,)
        cmd = "create edge %s from %s to %s" % (type,personneRIDA,personneRIDB)
        req = self.client.command( cmd )
    
    def get_membres_famille_from_personne(self, _personne):
        '''Retourne une array des membres de la famille d'une personne'''
        ## ORIENTDB Exemple d'un super gain de temps en passant directement par l adresse physique !
        #for c in self.client.command("select both('FAMILLE') from Personne where @RID = %s" % a)[0].both:
        #for c in self.client.command("select both('FAMILLE') from %s" % personne)[0].both:
        return self.client.command("select both('FAMILLE') from %s" % _personne)[0].both
    
    def close(self):
        self.client.db_close()

class PeuplerVilleNeo4J(PeuplerVille):
    def __init__(self, _nb_habitants):
        super(PeuplerVilleNeo4J, self).__init__(_nb_habitants)
        self.graph = Graph(os.environ["NEO4JDBSTR"])
        self.nb_habitations = self.graph.cypher.execute("match (n:Numero) return count(n) as count")[0].count
        
        self.nb_personnes = self.graph.cypher.execute("match (p:Personne) return count(p) as count")[0].count
        
        self.graph.cypher.execute("CREATE INDEX ON :Personne(nom)")
        self.graph.cypher.execute("CREATE INDEX ON :Personne(prenom)")
        self.graph.cypher.execute("CREATE INDEX ON :Personne(sexe)")
        self.graph.cypher.execute("CREATE INDEX ON :Personne(age)")
        
        # Pas de toBool pour les ETL ! correction donc
        self.graph.cypher.execute("match (n:Numero) where n.vide = \"true\" or n.vide = \"True\" set n.vide = True ")

        
    def habitationutilisee(self, _habitation):
        self.graph.cypher.execute( "match (n:Numero) where id(n) = %s set n.vide = False" % _habitation.id)
        
    def get_random_habitation(self,):
        '''Retourne une habitation(Numero) vide (le seul lien qu'elle a est le lien du Numero à sa rue) en random'''
        h = []  ## Parfois de facon sporadique on a un resultat vide de la requete suivante !!
        while len(h)==0:
            #h = self.graph.cypher.execute( "match (n:Numero) where not ()-[:HABITE]->(n) return id(n) as id,n as node SKIP %s LIMIT 1" %( random.randint(0,self.nb_habitations) ) )
            req = "match (n:Numero) where n.vide = True return id(n) as id,n as node SKIP %s LIMIT 5" %( random.randint(0,self.nb_habitations) )
            print "Cherche habitation vide : %s" % req
            h =  self.graph.cypher.execute( req )
        return  h[0]
    
    def get_habitation_byID(self, _habitation):
        return _habitation
    
    def get_random_personne(self,):
        '''Retourne une personne deja crée au hasard'''
        nb_personnes = self.graph.cypher.execute("match (p:Personne) return count(p) as count")[0].count
        p = []   ## Parfois de facon sporadique on a un resultat vide de la requete suivante !!
        while len(p) == 0:
            p = self.graph.cypher.execute( "match (n:Personne) return id(n) as id,n as node SKIP %s LIMIT 1" %( random.randint(0,self.nb_personnes) ) )
        return p[0]
    
    def habiter(self,_personne, _logement):
        '''Lie une personne(Personne) à un habitat (Numero)'''
        self.graph.create( Relationship(_personne.node, "HABITE", _logement.node) )
    
    def get_membres_famille_from_personne(self, _personne):
        '''Retourne une array des membres de la famille d'une personne'''
        #print "match (membre:Personne)-[r:FAMILLE]-(p:Personne) where id(p) = %s return id(membre) as id,membre as node" % _personne.id
        return self.graph.cypher.execute("match (membre:Personne)-[r:FAMILLE]-(p:Personne) where id(p) = %s return id(membre) as id,membre as node" % _personne.id)
        
    def creerLiensSociaux(self, _personneA, _personneB, _type="CONNAIT"):
        self.graph.create( Relationship(_personneA.node, _type, _personneB.node) )
    
    def creerHabitant(self, _personne):
        self.nb_habitants += 1
        self.nb_personnes += 1
        habitant = Node.cast("Personne", _personne.to_dict() )
        habitant, = self.graph.create(habitant)
        print ("    %s" % (habitant) )
        
        h = NeoPersonne()
        h.id = habitant._Node__id
        h.node = habitant
        return h
    
    
    
    def close(self):
        #self.graph.close()
        pass

class NeoPersonne(object):
    def __init__(self,):
        self.id = 0
        self.node = None

def usage():
    print "usage: %s orientdb | neo4j" % sys.argv[0]
    exit(1)

if __name__ == "__main__":
    
    ## Nombre d'habitants a loger
    NB_HABITANTS = 10000000
    
    if len(sys.argv) != 2:
        usage()
    if sys.argv[1] == "orientdb":
        peupler = PeuplerVilleOrientDB(NB_HABITANTS)
    elif sys.argv[1] == "neo4j":
        peupler = PeuplerVilleNeo4J(NB_HABITANTS)
    else:
        usage()
    
    struct_date1=datetime.datetime.now()
    peupler.peupler()
    struct_date2=datetime.datetime.now()
    diff = struct_date2 - struct_date1
    print affiche_temps("TOTAL", diff)
    
    
    #print peupler.get_random_habitation()
    # match (n:Numero),(p:Personne) where not (p)-[:HABITE]->(n) return n,p skip 14 limit 2
    # match (p:Personne),(n:Numero) where id(n) = 1277436 create (p)-[:HABITE]->(n) return p,n limit 10
    # create(:Personne {prenom:"Lili", age:30})
    
    peupler.close()
