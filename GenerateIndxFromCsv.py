#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
generate new idx from 
1. company owner
2. address (omit floor)

P.S: encooding in env must be UTF-8
"""

import requests
from datetime import datetime
import json
import sys
import traceback
from time import sleep
import requests

import threading
import os
import csv
from collections import Counter

if __name__ == '__main__':
   # sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    today=datetime.now()
    
    fmt = '%Y-%m-%d %H:%M:%S'
    print('Starting time: '+ today.strftime(fmt))


    if len(sys.argv) < 3:
        print(sys.argv[0] + ' INPUT_FILE OUTPUT_FILE' )
    else:
        # (self, skip1stLine=False, delimiter=None, idFieldNo=0)
        print("Start to Process...")
        # "id","name","addr","parent_id","indcode0","CLASSTHREE","capital","operiodatapr","pred1","h5","h1","h2","h3","h4","Outlook","yel_name","yel_addr","yel_tel","addr_eq_yel_addr","TFBcenter","assignAO","assignnote","owner"
        inputFileName=sys.argv[1]
        outputFileName=sys.argv[2]
   
        groupIdx=1
        groupArray=[]
        tmpArray=[]
        with open(inputFileName, 'r') as iFile:
                reader = csv.reader(iFile, dialect='excel', delimiter=',', doublequote=True)
                for row in reader:
                    address=row[2]
                    if len(row) >=22:
                        company_owner=row[22]
                    else:
                        company_owner=''
                    if company_owner in ('NULL', 'null'):
                        company_owner=''
                    if address != 'addr' and company_owner != '':
                        newIdx=company_owner+'_'+address.split('號')[0]+'號'
                    else:
                        newIdx=''
                    if len(row) == 22:
                        row.append('')
                    row.append(newIdx)
                    if newIdx != '':
                        groupArray.append(newIdx)
                    tmpArray.append(row)

       # sortGroupArray=sorted(groupArray, key=Counter(groupArray).get, reverse=True)
        dict={}
        for item in groupArray:
            if item in dict:
                dict[item]=dict[item]+1
            else:
                dict[item]=1
        
      #  print(dict)               
        with open(outputFileName, 'w', encoding='utf-8') as oFile:
            writer=csv.writer(oFile, dialect='excel', delimiter=',', doublequote=True)
            for row in tmpArray:
                if row[23] in dict and dict[row[23]] > 1:
                    tmpStr=row[23]
                    row.append(tmpStr)
                else:
                    row.append('')
                writer.writerow(row)


    today=datetime.now()
    print('End time: '+ today.strftime(fmt))

   
