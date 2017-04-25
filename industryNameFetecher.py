#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
用來從統一編號得到公司名稱/商業名
所使用的 URL: 
1. http://lasai.org/od/data/api/9D17AE0D-09B5-4732-A8F4-81ADED04B679?%24format=json&%24filter=Business_Accounting_NO%20eq%20統一編號
2. http://lasai.org/od/data/api/855A3C87-003A-4930-AA4B-2F4130D713DC?%24format=json&%24filter=President_No%20eq%2010統一編號

where format = json and {Business_Accounting_NO} is 統一編號
eg: http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?$format=xml&$filter=Business_Accounting_NO%20eq%2020828393

source file: SME_CLosed.csv

P.S: encodoing in env must be UTF-8
"""


from urllib.parse import urlparse
from io import TextIOWrapper
from datetime import datetime
import json
import sys
import traceback
from time import sleep
import threading
import requests
import copy


class ConnectionObject:
    def __init__(self, url, isCompany):
        self.url=url
        self.payload={}
        self.isCompany=isCompany
        
    def setupPayload(self, company_id):
        self.payload['$format']='json'
        
        if self.isCompany:
            self.payload['$filter']='Business_Accounting_NO eq ' + company_id
        else:
            self.payload['$filter'] = 'President_No eq ' + company_id
    def getResponse(self):
        try:
            response=requests.get(self.url, self.payload)
            response.encoding='UTF-8'
            #print(str(self.isCompany) + self.url)
            if response != None and response.text.strip() != '':
                if self.isCompany:
                    return response.json()[0]['Company_Name']
                else:
                    
                    return response.json()[0]['Business_Name']
        except Exception as e:
            print('url:',self.url)
            print('payload:',self.payload)
            print(response.text)
            traceback.print_exc()
            return ''



class IndustryNameGetter(threading.Thread):
    def __init__(self, threadID, sourceFileName, outputFileName,*connectionObjects):
        threading.Thread.__init__(self)
        self.threadID=threadID
        self.sourceFileName=sourceFileName
        self.outputFileName=outputFileName
        self.connectionObjects=[]
        for c in connectionObjects:
            self.connectionObjects.append(ConnectionObject(c.url, c.isCompany))
        

        #self.resultDisctionary={}
    def getTotalLines(self):
        try:
            num_lines = sum(1 for line in open(self.sourceFileName))
            return num_lines
        except Exception:
            raise
    def parse(self):
        totalLines=self.getTotalLines()
        try:
            index=0
            ##print(super().getSchemeAndHost(url))
            idxSuccess=0
            c=[]
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')
            ofile=open(self.outputFileName, mode='w', encoding="UTF-8")

            #print(restInfo)
            for line in f:
                setFlag=False
                cn='' # company name

                idx=0
                bao=line.strip().replace('\ufeff','')

                while (not setFlag) and (idx < len(self.connectionObjects)):
                    cb=self.connectionObjects[idx]
                    cb.setupPayload(bao)
                    
                    cn=cb.getResponse()
                    if cn != None and cn!='':
                        setFlag=True
                    idx+=1

                index+=1
                                            # company name
                ofile.write("%s,%s\n" %(bao, cn))
                ofile.flush()
                if setFlag:
                    idxSuccess+=1  
                print("Thread ID: %d, Parsing records(OtherInfoGetter): (%d/%d)\r" %(self.threadID,index, totalLines), flush=True)
                sys.stdout.flush()
            print("\rThread ID: %d report, Successful items: %d/Total items: %d \r" %(self.threadID, idxSuccess, totalLines))
        except IOError as inst: 
            print("Error happened when parsing the file: " + self.sourceFileName)
            raise
        except Exception as e:
            print("Unknown error")
            raise
        finally:
            # for connection in c:
            #     connection.close()
            f.close()
            ofile.close()
    def run(self):
        self.parse();



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


if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
    
    # the path of your source file and destination file
    # be sure to modify the following to reflect your file name (absolute path)
    pathName="./"

    # source file
    fileName="./1.csv"
    #fileName="./2.csv"
    #fileName=pathName+"./SME_Closed.csv"

    # how many threads you'd like to execute
    splitFileNum=2
    

    # Part I: get company name
    # urls that we will use
    co0=ConnectionObject('http://lasai.org/od/data/api/9D17AE0D-09B5-4732-A8F4-81ADED04B679', True)
    co1=ConnectionObject('http://lasai.org/od/data/api/855A3C87-003A-4930-AA4B-2F4130D713DC', False)

    splitFile(fileName, pathName, splitFileNum)

    index=0
    threads=[]
    for index in range(0, splitFileNum):
        threads.append(IndustryNameGetter(index, fileName+"_"+str(index), 'parse_'+str(index)+'.csv', co0, co1))

    index=0
    for index in range(0, splitFileNum):
        threads[index].start()

    index=0
    for index in range(0, splitFileNum):
        threads[index].join()
    
    
    # Part II: get the number of people in this company from 104


    

    

    