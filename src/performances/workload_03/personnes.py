#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ast
import csv
import os
import random
import re

from common import RandomPoids


class Personne(object):
    def __init__(self):
        self.nom = self.prenom = self.sexe = self.age = self.naissance = self.salaire = self.activite = None
        
    def to_json(self,):
        return "{ 'sexe':'%s','prenom':'%s','nom':'%s','age':'%s','naissance':'%s','activite':'%s','salaire':'%s' }" % (self.sexe,self.prenom,self.nom,self.age,self.naissance,self.activite,self.salaire)
    def to_dict(self):
        return ast.literal_eval( self.to_json() )

class Personnes(object):
    '''
    
    Prenom : http://opendata.paris.fr/explore/dataset/liste_des_prenoms_2004_a_2012/?tab=table
    Nom de famille : http://www.dicollecte.org/home.php?prj=fr
    
    Emploi : INSEE EmploiTotalAu31DecembreParSecteurDActiviteStatutEtSexe
    
    Relations : http://www.credoc.fr/pdf/Rech/C169.pdf
    Ego -> Famille -> Amis -> Collegues -> Connaissances
    Personnes par foyer : http://www.credoc.fr/pdf/Rech/C169.pdf
    1       24
    2       30
    3       17
    4       17
    5et+    9
    
    Moyenne nbre d'amis : http://www.scienceshumaines.com/les-amis-de-mes-amis_fr_10542.html
    de 4 a 9 moyenne 6
    
    Nombre de connaisances par personne : http://fr.wikipedia.org/wiki/Nombre_de_Dunbar
    150
    
    Salaire net en 2011 en France: http://www.inegalites.fr/spip.php?article1054
    salaire;qte
    1140;10
    1270;10
    1400;10
    1520;10
    1660;10
    1860;10
    2100;10
    2500;10
    3300;4
    4200;5
    7500;1
    
    Population Toulouse metropole 700 000
    Toulouse : 462000. 462000./65832= 7 personnes par numeros
    
    Ratio personnes/numeros d'habitation (maison/immeubles)
    0;1
    1;20
    2;35
    3;20
    4;30
    5;5
    20;6
    50;8
    100;5
    200;4
    
    '''
    def __init__(self):
        ## Ages et des sexes suivant la pyramides des ages
        self.genes = []
        poids_genes = []
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/France_PyramideDesAges_2011.csv','r'), delimiter=',', quotechar='"')
        for row in list(reader)[1:]:
            poids_genes.append(int(row[2]))
            self.genes.append({ 'sexe':'M', 'age':4+int(row[1]), 'annee_naissance':int(row[0]) })
            poids_genes.append(int(row[3]))
            self.genes.append({ 'sexe':'F', 'age':4+int(row[1]), 'annee_naissance':int(row[0]) })

        ## Prenoms suivant le nombre de naissances
        self.prenoms = { 'F':[] , 'M':[] }
        poids_prenoms = { 'F':[] , 'M':[] }
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/liste_des_prenoms_2004_a_2012.csv','r'), delimiter=';')
        for row in list(reader)[1:]:
            genre = row[2]
            if genre == "X" : genre = "F"
            poids_prenoms[genre].append(int(row[1]))
            self.prenoms[genre].append(row[0])
        
        ## Nom de famille
        self.noms = []
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/nomsdefamille.csv','r'), delimiter=',', quotechar='"')
        p = re.compile('^[a-zA-Zéèëçêàù]+$')
        for row in list(reader):
            if p.match(str(row[0])):
                self.noms.append(row[0])
        
        ## Activites pro
        self.activites = { 'F':[] , 'M':[] }
        poids_activites = { 'F':[] , 'M':[] }
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/activite.csv','r'), delimiter=';')
        for row in list(reader)[1:]:
            genre = row[0]
            poids_activites[genre].append(int(row[2]))
            self.activites[genre].append(row[1])

        ## Salaires
        self.salaire = []
        poids_salaire = []
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/salaires.csv','r'), delimiter=';')
        for row in list(reader)[1:]:
            poids_salaire.append(int(row[1]))
            self.salaire.append(int(row[0]))
            
        ## Generation des personnes
        self.random_genes = RandomPoids(poids_genes)
        self.random_prenoms = { 'F': RandomPoids(poids_prenoms['F']), 'M': RandomPoids(poids_prenoms['M']) }
        self.random_activites = { 'F': RandomPoids(poids_activites['F']), 'M': RandomPoids(poids_activites['M']) }
        self.random_salaires = RandomPoids(poids_salaire)
    
    
    def get_nom(self,):
        return random.choice(self.noms).capitalize()
    
    def next(self, nom=None):
        '''
        Genere une nouvelle personne
        '''
        gene = self.genes[self.random_genes.get()]
        new = Personne()
        new.sexe = gene['sexe']
        new.age = gene['age']
        new.naissance = "%.2d/%.2d/%4d" % (random.randint(1,28), random.randint(1,12), gene['annee_naissance'])
        new.prenom = self.prenoms[new.sexe][self.random_prenoms[new.sexe].get()]
        new.nom = nom if nom else self.get_nom()
        
        if new.age >= 20:
            new.salaire = self.salaire[self.random_salaires.get()]
        else:
            new.salaire = 0
        
        if new.age >= 20 and new.age <= 63:
            new.activite = self.activites[new.sexe][self.random_activites[new.sexe].get()]
        elif new.age < 3:
            new.activite = "bebe"
        elif new.age < 10:
            new.activite = "enfant"
        elif new.age < 18:
            new.activite = "eleve"
        elif new.age < 25:
            new.activite = "etudiant"
        else :
            new.activite = "retraite"
        
        return new
        
    #def __str__(self):
    #    return '''      '''

class Famille(object):
    def __init__(self,):
        ## Personnes par famille
        self.personnesparfamille = []
        poids_personnesparfamille = []
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/personnesparfamille.csv','r'), delimiter=';')
        for row in list(reader)[1:]:
            poids_personnesparfamille.append(int(row[1]))
            self.personnesparfamille.append(int(row[0]))
        self.random_personnesparfamille = RandomPoids(poids_personnesparfamille)
        
    def next(self, personnes):
        '''
        Generation d'une nouvelle famaille
        '''
        famille = []
        nom = personnes.get_nom()
        ## Ajout des membres dans la famille de random membres
        while len(famille) < self.personnesparfamille[self.random_personnesparfamille.get()]:
            famille.append( personnes.next(nom) )
        return famille


class Habitation(object):
    def __init__(self,):
        self.personnes = Personnes()
        self.famille = Famille()
        #for i in range(0,100): print( "%s" % personnes.next().to_json() )
        
        ## Familles par habitation
        self.famillesparhabitation = []
        poids_famillesparhabitation = []
        reader = csv.reader(open(os.environ['BENCH_DATA']+'/famillesparhabitation.csv','r'), delimiter=';')
        for row in list(reader)[1:]:
            poids_famillesparhabitation.append(int(row[1]))
            self.famillesparhabitation.append(int(row[0]))
        self.random_familles = RandomPoids(poids_famillesparhabitation)
        
        
    def next(self,):
        print ("#### Habitation ####")
        familles = []
        nbf = 0
        while nbf < self.famillesparhabitation[self.random_familles.get()]:
            nbf += 1
            familles.append( self.famille.next(self.personnes) )
        return familles


if __name__ == "__main__":
    hab = Habitation()
    ## Nombre d'habitations a peupler
    NUMS_HABITATIONS = 10
    
    c = 0
    while c < NUMS_HABITATIONS:
        c += 1
        for f in hab.next():
            print "  famille:"
            for p in f:
                print p.to_json()
#
