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
        fieldNames=['id','name','addr','parentid','indcode0','CLASSTHREE','capital','operiodatapr','pred1','h5','h1','h2','h3','h4','Outlook','owner','idx']
        inputFileName=sys.argv[1]
        outputFileName=sys.argv[2]
   
        
        with open(inputFileName, 'r') as iFile:
            with open(outputFileName, 'w', encoding='utf-8') as oFile:
                reader = csv.reader(iFile)
                writer=csv.writer(oFile)
                for row in reader:
                    address=row[2]
                    if len(row) >=16:
                        company_owner=row[15]
                    else:
                        company_owner=''
                    if company_owner in ('NULL', 'null'):
                        company_owner=''
                    if address != 'addr' and company_owner != '':
                        newIdx=company_owner+'_'+address.split('號')[0]+'號'
                    else:
                        newIdx=''
                    if len(row) == 15:
                        row.append('')
                    row.append(newIdx)
         #           print(row)
                    writer.writerow(row)
    today=datetime.now()
    print('End time: '+ today.strftime(fmt))

   
