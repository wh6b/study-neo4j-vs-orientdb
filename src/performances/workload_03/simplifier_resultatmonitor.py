#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import datetime
import operator
import os
import re
import sys
import time
import numpy as np

def usage():
    print "Usage : %s <file_input> add|moy periode_agregation [sepateur_de_colonnes_CSV]" % sys.argv[0]
    print "add : Simplifie le fichier CSV en additionnant les valeurs toutes les <periode_agregation> lignes"
    print "moy : Realise une moyenne de valeurs toutes les <periode_agregation> lignes"
    print "  exemple: .. fichier1000lignes moy 100 genere un fichier de 10 lignes avec ligne 1 une moyenne des valeurs des lignes 1 a 1000, ligne N une moyenne des valeurs des lignes 1001 a 2000 etc." 
    sys.exit(1)

if len(sys.argv) < 4 or len(sys.argv) > 5: usage()
csvfile = sys.argv[1]
ACTION  = sys.argv[2]
PAS     = int(sys.argv[3])
SEP=sys.argv[4] if len(sys.argv) == 5 else ';'

if ACTION not in ('add','moy'): usage()
outfile = "%s.%s.modifie" % (csvfile,ACTION)
try:
    f = open(outfile,'w')
    f.write( open(csvfile,'r').readlines()[0] )
    file_in = open(csvfile,'r')
    PAS + 0
except Exception as e:
    print e
    usage()


line = 0
bufferA = None
bufferB = []
for row in file_in.readlines()[1:]:
    line += 1
    rowA= [int(i) for i in row.split(SEP) if re.match("^[0-9]+", i) ]
    #print "mod=%s" % (line % PAS)
    
    if bufferA:
        bufferA = map(operator.add, bufferA,rowA)
        bufferB.append(rowA)
    else:
        bufferA = rowA
        bufferB.append(rowA)
    #print "                %s" % rowA
    #print "bufferA=        %s" % bufferA
    if line % PAS == PAS-1:
        
        #print "LINE=%s" % ",".join(buffer1)
        #print "       bufferA= %s" % bufferA
        print "ECRIRE   %s     = %s;" % (line, SEP.join( [str(i) for i in bufferA ]) )
        if ACTION == "add" : 
            f.write(  "%s;\n" % SEP.join( [str(i) for i in bufferA ] )  )
        elif ACTION == "moy" : 
            mat = np.array(bufferB)
            print mat
            moy = mat.mean(axis=0)
            f.write(  "%s;\n" % SEP.join( [str(i) for i in moy ] )  )
        bufferA = None
        bufferB = []

f.close()
file_in.close()
