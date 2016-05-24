#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast

class Numero(object):
    '''
    no;lib_off;sti;nrivoli;rivoli;X_WGS84;Y_WGS84
    '''
    def __init__(self, _no, _lib_off, _sti, _nrivoli, _rivoli, _X_WGS84=None, _Y_WGS84=None):
        self.no = _no
        self.lib_off = _lib_off
        self.sti = _sti
        self.nrivoli = _nrivoli   ## rivoli sans lettres (la derniere)
        self.rivoli = _rivoli
        self.vide = True
        
        if _X_WGS84: self.X_WGS84 = _X_WGS84
        if _Y_WGS84: self.Y_WGS84 = _Y_WGS84
        
    def to_json(self):
        try:
            return "{ 'no':'%d', 'lib_off':'%s', 'sti':'%s', 'nrivoli':'%s', 'rivoli':'%s', 'X_WGS84':'%s', 'Y_WGS84':'%s', 'vide' : '%s' }" % (self.no,self.lib_off,self.sti,self.nrivoli,self.rivoli,self.X_WGS84,self.Y_WGS84,self.vide)
        except :
            return "{ 'no':'%d', 'lib_off':'%s', 'sti':'%s', 'nrivoli':'%s', 'rivoli':'%s', 'vide' : '%s' }" % (self.no,self.lib_off,self.sti,self.nrivoli,self.rivoli,self.vide)
    
    def to_dict(self):
        return ast.literal_eval( self.to_json() )
    
    def __str__(self):
        self.to_json()

class Voie(object):
    '''
    libelle;libelle_complet;commune;code_insee;sti;rivoli
    '''
    def __init__(self, _libelle, _libelle_complet, _commune, _code_insee, _sti, _rivoli):
        self.libelle = _libelle
        self.libelle_complet = _libelle_complet
        self.commune = _commune
        self.code_insee = _code_insee
        self.sti = _sti
        self.rivoli = _rivoli
    def to_json(self):
        return "{ 'libelle':'%s', 'libelle_complet':'%s', 'commune':'%s', 'code_insee':'%s', 'sti':'%s', 'rivoli':'%s' }" % (self.libelle,self.libelle_complet,self.commune,self.code_insee,self.sti,self.rivoli)
    def __str__(self):
        self.to_json()
