#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import datetime
import os
from py2neo import Graph, Node, Relationship
import pyorient
from random import random
import sys
import re
import time

from bisect import bisect_right

from common import RandomPoids
from common import Numero
from common import Voie
from common import affiche_temps

class CreerNumeros(object):
    '''
    Classe parent commune pour creer des numeros dans les voies d'une commune donnees            ﻿no
    Voies.csv:
    libelle;libelle_complet;commune;code_insee;sti;rivoli
    '''
    def __init__(self, nums_dans_rue):
        ## Numeros d'habitations par rue
        self.nums_dans_rue = nums_dans_rue
        
        ## Recuperer les noms des rues de toutes les communes
        self.voies = {}
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/Voies.clean.csv','r'), delimiter=';')
        
        communes = []
        for row in list(reader)[1:]:
            if row[2] not in self.voies.keys(): self.voies[row[2]] = []
            self.voies[row[2]].append( Voie(row[0], row[1], row[2], row[3], row[4], row[5]) )
            communes.append(row[2])
        
        ## Liste des communes
        self.communes = set(communes)

class CreerNumerosOrientDB(CreerNumeros):
    '''
    Creer des numeros dans les voies d'une commune donnees pour OrientDB
    '''
    def __init__(self, nums_dans_rue):
        self.client = pyorient.OrientDB(os.environ['ORIENTDBSERV'], int(os.environ['ORIENTDBPORT']))
        self.session_id = self.client.connect( os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        self.client.db_open( os.environ['ORIENTDBBASE'], os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        
        self.client.command( "select @RID from Numero limit 1" )
        
        super(CreerNumerosOrientDB, self).__init__(nums_dans_rue)
        
    def peuple(self, commune):
        voiesdelacommune = self.voies[commune]
        nbvoiesdelacommune = len(self.voies[commune])
        placecommune = list(self.communes).index(commune)+1
        nbcommunes = len(self.communes)
        cpt = 0
        
        for v in voiesdelacommune:
            numero = Numero(7, v.libelle, v.sti, v.rivoli[:-1], v.rivoli, None, None)
            cpt += 1
            ## Probleme avec Voies.clean.csv:AV HONORE SERRES;Avenue Honoré Serres;TOULOUSE;31555;315556614424;3105554256M; non créée ainsi qu'une autre
            try:
                voie, = self.client.command( "select from Voie where libelle = '%s' and in(ORGANISE)['libcom'] = '%s'" %(numero.lib_off, commune) )
            except ValueError as e:
                print("WARNING;%s;  La voie <%s> de la commune <%s> n'existe pas." % (cpt,numero.lib_off,commune) )
                print("%s" % (e) )
                continue
            
            
            for n in self.nums_dans_rue:
                numero.no = n
                numero.vide = True
                newnum = self.client.command( "create vertex Numero  content %s" % numero.to_json() )

                cmd = "create edge COMPORTE  from %s to %s" % (newnum[0]._rid, voie._rid)
                #if n % 50 == 0 : print( "%s : %s" % (n,cmd) )
                req = self.client.command( cmd )
                #if n % 50 == 0 : print( "%s" % req[0]._rid)         ## list index out of range ?!!! ou ca ?
                
                #dir(object)
                #rec_position1.__dict__
                #newnum[0]._rid
            if cpt %10 == 0 : print( "Voie %s/%s de la commune %s / %s" % (cpt,nbvoiesdelacommune, placecommune, nbcommunes) )
            
    def close(self):
        self.client.db_close()


class CreerNumerosNeo4J(CreerNumeros):
    '''
    Creer des numeros dans les voies d'une commune donnees pour Neo4J
    match (v:Voie)-[]-(c:Commune) where v.libelle = "RUE DES PINSONS" and c.libcom <> "TOULOUSE"  return c,v
    
    match (v:Voie)-[]-(c:Commune) where v.libelle = "RTE DE LAVALETTE" and c.libcom = "BEAUPUY"  return c,v
    
    self.graph.cypher.execute("")
    '''
    def __init__(self, nums_dans_rue):
        self.graph = Graph(os.environ["NEO4JDBSTR"])
        
        super(CreerNumerosNeo4J, self).__init__(nums_dans_rue) 

    def peuple(self, commune):
        voiesdelacommune = self.voies[commune]
        nbvoiesdelacommune = len(self.voies[commune])
        placecommune = list(self.communes).index(commune)+1
        nbcommunes = len(self.communes)
        cpt = 0
        for v in voiesdelacommune:
            #if not re.compile(".*D59.*").match(v.libelle): continue
            numero = Numero(7, v.libelle, v.sti, v.rivoli[:-1], v.rivoli, None, None)
            cpt += 1
            ## Pour contourner un bug d'espace dans le fichier d'open data..
            if numero.lib_off[0] == " ": numero.lib_off = numero.lib_off[1:]
            try:
                ## On recupere le noeud de rue qui va recevoir ce numero. Ne retourne normalement qu'une rue donc on selectionne le premier et unique resultat avec ,
                #voie, = self.graph.cypher.execute( "match (v:Voie { libelle:'%s' })-[:ORGANISE]-(c:Commune {libcom:'%s'}) return v" % (numero.lib_off, commune) )
                voie, = self.graph.cypher.execute( "match (v:Voie { libelle:'%s' })-[:ORGANISE]-(c:Commune {libcom:'%s'}) return v" % (numero.lib_off, commune) )
            except ValueError as e:
                print("WARNING;%s;  La voie <%s> de la commune <%s> n'existe pas." % (cpt,numero.lib_off,commune) )
                print("%s" % (e) )
                continue
            
            for n in self.nums_dans_rue:
                numero.no = n
                newnum = Node.cast("Numero", numero.to_dict() )
                #print( "    %s ; %s ; %s\n%s" % (n,type(newnum),newnum,voie) )
                ## Le noeud Numero est crée en meme temps que la relation
                arete = self.graph.create( Relationship(voie['v'],"COMPORTE",newnum) )
                
                #if n % 50 == 0 : print( "%s ; %s ; %s" % (n,type(newnum),newnum) )
                #if n % 50 == 0 : print( "  %s" % (arete) )
            if cpt %10 == 0 : print( "Voie %s/%s de la commune %s / %s" % (cpt,nbvoiesdelacommune, placecommune, nbcommunes) )

    def close(self):
        #self.graph.close()
        pass


def usage():
    print "usage: %s orientdb | neo4j" % sys.argv[0]
    exit(1)

if __name__ == "__main__":
    
    ## Numeros d'habitations par rue
    NUMS_DANS_RUE = range(1,101)
    
    if len(sys.argv) != 2:
        usage()
    if sys.argv[1] == "orientdb":
        voies = CreerNumerosOrientDB(NUMS_DANS_RUE)
    elif sys.argv[1] == "neo4j":
        voies = CreerNumerosNeo4J(NUMS_DANS_RUE)
    else:
        usage()
    
    t1=datetime.datetime.now()
    for commune in voies.communes:
        ## Toulouse est déjà remplie de numeros avec le Workload_01
        if commune != "TOULOUSE":
            voies.peuple(commune)

    ligne = affiche_temps("DUREE;%s" % sys.argv[1], datetime.datetime.now() - t1)
    
    filename = "%s/workload_02/compteurs.workload_02.%s.csv" % (os.environ['BENCH_HOME'],sys.argv[1])
    f = open(filename,'a')
    f.write("%s\n" % ligne)
    f.close()
    print ligne
    
    voies.close()

    
    
    
    
    
    
    
    
    
    
    
    
    #oui
