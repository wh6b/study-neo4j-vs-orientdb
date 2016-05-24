#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
from py2neo import Graph, Node, Relationship
import pyorient
import random
import re
import sys
import time

from common import affiche_temps

class Monitor(object):
    '''
    
    '''
    def __init__(self, _nb_check, _intervalle):
        self.nb_check=_nb_check
        self.intervalle=_intervalle
        ## Compteurs des actions sur la base de donnees
        self.anciens_compteurs = {}
        self.compteurs = { 'noeud':{ 'R':0,'W':0}, 'arete':{ 'R':0,'W':0} }
        
        self.anciens_total_operations = {}
        self.total_operations = { 'R':0,'W':0 }
        
        self.anciens_compteurs_details = {}
        self.compteurs_details = { 'personne':{ 'R':0,'W':0}, 'CONNAIT':{ 'R':0,'W':0},'FAMILLE':{ 'R':0,'W':0},'AMI':{ 'R':0,'W':0} }
        
        #self.filename = "compteurs.workload_03.%s.csv" % time.strftime("%Y%m%d.%H%M%S")
        if not os.path.isfile(self.filename) :
            ligne = "%s;%s;%s;%s;;%s;%s;%s;%s;%s;%s;%s;%s;;%s;%s;%s;%s;%s;%s;%s;%s" % (
                    "TOTAL_R", "TOTAL_W",
                    "DELTA TOTAL_R/s", "DELTA TOTAL_W/s",
                    "NB noeuds_R", "NB noeuds_W", 
                    "NB aretes_R", "NB aretes_W",
                    "noeud_R/s", "noeud_W/s",
                    "arete_R/s", "arete_W/s",
                    "personne_R/s", "personne_W/s",
                    "CONNAIT_R/s", "CONNAIT_W/s",
                    "FAMILLE_R/s", "FAMILLE_W/s",
                    "AMI_R/s", "AMI_W/s",
                )
            self.ecrit(ligne)

        
    def ecrit(self, _ligne):
        f = open(self.filename,'a')
        f.write("%s\n" % _ligne)
        f.close()
        print _ligne
    
    def monitor(self,):
        for a in range(0,self.nb_check):
            personne, connait, famille, ami = self.check()
            
            self.anciens_total_operations = self.total_operations
            self.anciens_compteurs = self.compteurs
            self.anciens_compteurs_details = self.compteurs_details
            
            noeuds = personne
            arretes = connait + famille + ami
            
            self.total_operations = { 'R':arretes,'W':noeuds+arretes }
            self.compteurs = { 'noeud':{ 'R':arretes,'W':noeuds}, 'arete':{ 'R':0,'W':arretes} }
            self.compteurs_details = { 'personne':{ 'R':arretes,'W':personne}, 'CONNAIT':{ 'R':0,'W':connait},'FAMILLE':{ 'R':0,'W':famille},'AMI':{ 'R':0,'W':ami} }
            
            ligne = "%d;%d;%d;%d;;%d;%d;%d;%d;%d;%d;%d;%d;;%d;%d;%d;%d;%d;%d;%d;%d" % (
                self.total_operations['R'],
                self.total_operations['W'],
                (self.total_operations['R'] - self.anciens_total_operations['R'])/self.intervalle,
                (self.total_operations['W'] - self.anciens_total_operations['W'])/self.intervalle,
                
                self.compteurs['noeud']['R'],
                self.compteurs['noeud']['W'],
                self.compteurs['arete']['R'],
                self.compteurs['arete']['W'],
                (self.compteurs['noeud']['R'] - self.anciens_compteurs['noeud']['R'])/self.intervalle,
                (self.compteurs['noeud']['W'] - self.anciens_compteurs['noeud']['W'])/self.intervalle,
                (self.compteurs['arete']['R'] - self.anciens_compteurs['arete']['R'])/self.intervalle,
                (self.compteurs['arete']['W'] - self.anciens_compteurs['arete']['W'])/self.intervalle,
                
                (self.compteurs_details['personne']['R'] - self.anciens_compteurs_details['personne']['R'])/self.intervalle,
                (self.compteurs_details['personne']['W'] - self.anciens_compteurs_details['personne']['W'])/self.intervalle,
                (self.compteurs_details['CONNAIT']['R'] - self.anciens_compteurs_details['CONNAIT']['R'])/self.intervalle,
                (self.compteurs_details['CONNAIT']['W'] - self.anciens_compteurs_details['CONNAIT']['W'])/self.intervalle,
                (self.compteurs_details['FAMILLE']['R'] - self.anciens_compteurs_details['FAMILLE']['R'])/self.intervalle,
                (self.compteurs_details['FAMILLE']['W'] - self.anciens_compteurs_details['FAMILLE']['W'])/self.intervalle,
                (self.compteurs_details['AMI']['R'] - self.anciens_compteurs_details['AMI']['R'])/self.intervalle,
                (self.compteurs_details['AMI']['W'] - self.anciens_compteurs_details['AMI']['W'])/self.intervalle,
            )
            ## Pour ne pas avoir les anciens compteurs a 0 avec des grosses valeurs au passage suivant lors du premier passage
            if a > 1:
                self.ecrit(ligne)
            else:
                self.ecrit("")
            
            time.sleep(self.intervalle)
            



class MonitorOrientDB(Monitor):
    def __init__(self, _nb_check, _intervalle):
        self.filename = "%s/workload_03/compteurs.workload_03.orientdb.csv" % os.environ['BENCH_HOME']
        super(MonitorOrientDB, self).__init__(_nb_check, _intervalle)
        
        self.client = pyorient.OrientDB(os.environ['ORIENTDBSERV'], int(os.environ['ORIENTDBPORT']))
        self.session_id = self.client.connect( os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        self.client.db_open( os.environ['ORIENTDBBASE'], os.environ['ORIENTDBUSER'], os.environ['ORIENTDBPASS'] )
        
        self.clusterpersonneid = int(self.client.command( "select from Personne limit 1")[0]._rid.split(':')[0][1:] )
        self.clusterhabitationid = int(self.client.command( "select from Numero limit 1")[0]._rid.split(':')[0][1:] )
        self.clusterconnaitid =  int(self.client.command( "select from Connait limit 1" )[0]._rid.split(':')[0][1:] )
        self.clusterfamilleid =  int(self.client.command( "select from Famille limit 1" )[0]._rid.split(':')[0][1:] )
        self.clusteramiid =      int(self.client.command( "select from Ami limit 1"     )[0]._rid.split(':')[0][1:] )
        
        

    def check(self,):
        return [ self.client.data_cluster_count( [self.clusterpersonneid] ),
            self.client.data_cluster_count( [self.clusterconnaitid] ), self.client.data_cluster_count( [self.clusterfamilleid] ), self.client.data_cluster_count( [self.clusteramiid] ),
        ]
        
    def close(self):
        self.client.db_close()

class MonitorNeo4J(Monitor):
    def __init__(self, _nb_check, _intervalle):
        self.filename = "%s/workload_03/compteurs.workload_03.neo4j.csv" % os.environ['BENCH_HOME']
        super(MonitorNeo4J, self).__init__(_nb_check, _intervalle)
        self.graph = Graph(os.environ["NEO4JDBSTR"])

    def check(self,):
        return [
            self.graph.cypher.execute("match (p:Personne) return count(p) as count")[0].count,
            self.graph.cypher.execute("match (p:Personne)-[r:CONNAIT]->() return count(r) as count")[0].count,
            self.graph.cypher.execute("match (p:Personne)-[r:FAMILLE]->() return count(r) as count")[0].count,
            self.graph.cypher.execute("match (p:Personne)-[r:AMI]->() return count(r) as count")[0].count,
        ]
  
    
    def close(self):
        #self.graph.close()
        pass

def usage():
    print "usage: %s orientdb | neo4j" % sys.argv[0]
    exit(1)

if __name__ == "__main__":
    ## Nombre de check
    NB_CHECK=50000
    ## Intervalle entre les checks
    INTER=5
    
    if len(sys.argv) != 2:
        usage()
    if sys.argv[1] == "orientdb":
        m = MonitorOrientDB(NB_CHECK,INTER)
    elif sys.argv[1] == "neo4j":
        m = MonitorNeo4J(NB_CHECK,INTER)
    else:
        usage()
    
    m.monitor()
    
    m.close()
