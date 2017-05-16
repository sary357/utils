#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Some utilty functions

P.S: encodoing in env must be UTF-8
"""
from datetime import datetime
def splitFile(fileName, tempOutFolder, numTempFiles):
    oFiles=[]
    try:
        ifile=open(fileName, mode='r', buffering=-1, encoding='UTF-8')
        idx=0
        while idx < numTempFiles:
            ofile=open(tempOutFolder+"/"+fileName+"_"+str(idx), mode='w')
            oFiles.append(ofile)
            idx+=1

        idx=0
        for line in ifile:
            if idx==numTempFiles:
                idx=0
            stsLine=line.strip().replace('\ufeff','')
            if len(stsLine) < 8 and len(stsLine)>2:
                num="0"*(8-len(stsLine))
                stsLine=num + stsLine
            oFiles[idx].write(stsLine+'\n')
            idx+=1
    except Exception as e:
        raise
    finally:
        idx=0
        for f in oFiles:
            f.close()
        ifile.close()

def get8DigitCompanyId(company_id):
    bao=company_id
    bao=bao.strip().replace('\ufeff','')
    if len(bao) <8:
        bao="0"*(8-len(bao))+bao
    return bao

def getTmpFileNamePostfix():
    currentTime=datetime.now()
    tmpFilePostfix=currentTime.strftime('%Y%m%d%H%M%S')
    return tmpFilePostfix