#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import datetime
from random import random
from bisect import bisect_right



class RandomPoids(object):
    '''
    Retourne l'indice d'une liste de poids (probalités ou quantités) au hasard en fonction de la probabilité 
    '''
    def __init__(self, liste_poids):
        pointeur = 0
        self.echantillons = []
        for poids in liste_poids:
            pointeur += poids
            self.echantillons.append(pointeur)

    def get(self):
        rand = random() * self.echantillons[-1]
        return bisect_right(self.echantillons, rand)




def affiche_temps(_msg, _duree):
    ''' Indique combien de temps une recherche a duree'''
    duree= _duree.seconds * 1000 + _duree.microseconds/1000
    duree_str = "%s s %s ms" % (_duree.seconds, _duree.microseconds/1000)
    return "%s;%s;%s" % (_msg, duree, duree_str,)
