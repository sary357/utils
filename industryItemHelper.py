#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
用來從統一編號得到營業項目代碼
所使用的 URL: http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}

where format = json and {Business_Accounting_NO} is 統一編號
eg: http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?$format=xml&$filter=Business_Accounting_NO%20eq%2020828393

source file: SME_CLosed.csv

P.S: encodoing in env must be UTF-8
"""

from abc import ABCMeta, abstractmethod
from http.client import HTTPConnection
from http.client import HTTPException
from urllib.parse import urlparse
from io import TextIOWrapper
from datetime import datetime
import json
import sys
import traceback
from time import sleep
import requests
import threading
from utils import get8DigitCompanyId
from utils import splitFile

from ConnectionObject import ConnectionObject

# def splitFile(fileName, tempOutFolder, numTempFiles):
#     oFiles=[]
#     try:
#         ifile=open(fileName, mode='r', buffering=-1, encoding='UTF-8')
#         idx=0
#         while idx < numTempFiles:
#             ofile=open(tempOutFolder+"/"+fileName+"_"+str(idx), mode='w')
#             oFiles.append(ofile)
#             idx+=1

#         idx=0
#         for line in ifile:
#             if idx==numTempFiles:
#                 idx=0
#             stsLine=line.strip().replace('\ufeff','')
#             if len(stsLine) < 8 and len(stsLine)>2:
#                 num="0"*(8-len(stsLine))
#                 stsLine=num + stsLine
#             oFiles[idx].write(stsLine+'\n')
#             idx+=1
#     except Exception as e:
#         raise
#     finally:
#         idx=0
#         for f in oFiles:
#             f.close()
#         ifile.close()

# def get8DigitCompanyId(company_id):
#     bao=company_id
#     bao=bao.strip().replace('\ufeff','')
#     if len(bao) <8:
#         bao="0"*(8-len(bao))+bao
#     return bao

# class ConnectionObject:
#     def __init__(self, url, isCompany):
#         self.url=url
#         self.payload={}
#         self.isCompany=isCompany
        
#     def setupPayload(self, company_id):
#         self.payload['$format']='json'
        
#         if self.isCompany:
#             self.payload['$filter']='Business_Accounting_NO eq ' + company_id
#         else:
#             self.payload['$filter'] = 'President_No eq ' + company_id
#     def getResponse(self):
#         try:
#             response=requests.get(self.url, self.payload)
#             response.encoding='UTF-8'
#             #print(str(self.isCompany) + self.url)
#             #print(response.text)
#             if response != None and response.text.strip() != '':
#                 return response.json()
#             else:
#                 return None
#         except Exception as e:
#             print('url:',self.url)
#             print('payload:',self.payload)
#             print(response.text)
#             traceback.print_exc()
#             return None


class OpenDataBaseParser( threading.Thread, metaclass=ABCMeta): 
    def __init__(self, threadID, sourceFileName, outputFileName,*connectionObjects):
        threading.Thread.__init__(self)
        self.sourceFileName=sourceFileName
        self.outputFile=outputFileName
        self.threadID=threadID
        self.resultDisctionary={}
        self.connectionObjects=[]
        for c in connectionObjects:
            self.connectionObjects.append(ConnectionObject(c.url, c.isCompany))

    @abstractmethod
    def parse(self):
        pass

    def getTotalLines(self):
        try:
            num_lines = sum(1 for line in open(self.sourceFileName))
            return num_lines
        except Exception:
            raise

    def getOutput(self):
        print("Dump output file: %s"%(self.outputFile,))
        keyList=sorted(self.resultDisctionary)
        try:
            f=open(self.outputFile, mode='w', encoding="UTF-8")
            for key in keyList:
                #print("%s,%s" %(key, self.resultDisctionary[key]))
                f.write("%s,%s\n" %(key, self.resultDisctionary[key]))
        except Exception:
            print("Error happened when dumping output")
            raise
        finally:
            f.close()

    def run(self):
        self.parse();


class OtherInfoGetter(OpenDataBaseParser):
    def parse(self):
        totalLines=super().getTotalLines()
        index=0
        ##print(super().getSchemeAndHost(url))
        idxSuccess=0
        c=[]
        maxRetryCount=2 # max retry count
        retryIdx=0
        try:
            
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')
            ofile=open(self.outputFile, mode='w', encoding="UTF-8")

            #print(restInfo)
            for line in f:
                setFlag=False
                csa='' # capital stock amount
                cl='' # company location
                csd='' # company setup date
                rkd='' # revoke date
                sbd='' # suspend beginning date
                sed='' # suspend end date

                bao=get8DigitCompanyId(line)

                while (retryIdx < maxRetryCount) and (not setFlag):
                    idx=0
                    while (not setFlag) and (idx < len(self.connectionObjects)):
                        cb=self.connectionObjects[idx]
                        cb.setupPayload(bao)
                        sourceJson=cb.getResponse()

                        if sourceJson!=None:
                            item=sourceJson[0]
                            if "Company_Setup_Date" in item:
                                csd=str(item["Company_Setup_Date"]).strip()
                                setFlag=True
                            if "Capital_Stock_Amount" in item:
                                csa=str(item["Capital_Stock_Amount"]).strip()
                                setFlag=True
                            if "Company_Location" in item:
                                cl=str(item["Company_Location"]).strip()
                                setFlag=True
                            if "Business_address" in item:
                                cl=str(item["Business_address"]).strip()
                                setFlag=True
                            if "Revoke_App_Date" in item:
                                rkd=str(item["Revoke_App_Date"]).strip()
                                setFlag=True
                            if "Sus_Beg_Date" in item:
                                sbd=str(item["Sus_Beg_Date"]).strip()
                                setFlag=True
                            if "Sus_End_Date" in item:
                                sbd=str(item["Sus_End_Date"]).strip()
                                setFlag=True
                        idx+=1
                index=index+1
                                            # company location, company stock amount, company setup date, revoke date, suspend beginning date, suspend end date
                ofile.write("%s,%s\n" %(bao, cl+","+csa+","+csd+','+rkd+','+sbd+','+sed))
                ofile.flush()
                if setFlag:
                    idxSuccess+=1  
                print("Thread ID: %d, Parsing records(OtherInfoGetter): (%d/%d)\r" %(self.threadID, index, totalLines), flush=True)
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
    def getOutput(self):
        print("Dump output file: %s"%(self.outputFile,))

class IndustryCategoryGetter(OpenDataBaseParser):

    def parse(self):
        totalLines=super().getTotalLines()
        index=0
        idxSuccess=0
        c=[]
        maxRetryCount=2 # max retry count
        retryIdx=0
        try:

            ##print(super().getSchemeAndHost(url))
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')          

            #print(restInfo)
            for line in f:
                setFlag=False
                cbItem=''
                bao=get8DigitCompanyId(line)

                while (retryIdx < maxRetryCount) and (not setFlag):
                    idx=0
                    while (not setFlag) and (idx < len(self.connectionObjects)):
                        cb=self.connectionObjects[idx]
                        cb.setupPayload(bao)
                        tempList=cb.getResponse()
                        #print(tempList)

                        if tempList!=None and tempList[0] != None:
                            if "Cmp_Business" in tempList[0]:
                                categoryList=tempList[0]['Cmp_Business']
                                for category in  categoryList: 
                                    if category["Business_Seq_NO"] == "0001" :   
                                        cbItem= str(category["Business_Item"]).strip()+","              
                                        cbItem= cbItem+str(category["Business_Item_Desc"]).strip()              
                                        setFlag=True
                            if ("Business_Item_Old" in tempList[0]) and (len(tempList[0]['Business_Item_Old']) > 0 ):
                                for item in tempList[0]['Business_Item_Old']:
                                    if item['Business_Seq_No'] == '1':
                                        cbItem=str(item['Business_Item']).strip()+","
                                        cbItem=cbItem+str(item['Business_Item_Desc']).strip()      
                                        setFlag=True

                        idx+=1
                if setFlag:
                    idxSuccess+=1                             
                                    
                index=index+1
                self.resultDisctionary[bao]=cbItem.replace('\n', '')
                print("Thread ID: %d, Parsing records(IndustryCategoryGetter): (%d/%d)\r" %(self.threadID, index, totalLines), flush=True)
                sys.stdout.flush()
            print("\rThread ID: %d, Successful items: %d/Total items: %d \r" %(self.threadID, idxSuccess, totalLines))
        except IOError as inst: 
            print("Error happened when parsing the file: " + self.sourceFileName)
            raise
        except HTTPException as e:
            print("Error happened when get the response from remote URL")
            raise
        except Exception as e:
            print("Unknown error")
            raise
        finally:
            
            f.close()



if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
    
    # the path of your source file and destination file
    pathName="./"

    # be sure to modify the following to reflect your file name (absolute path)
    #fileName=pathName+"./SME_Closed.csv"
    fileName="./1.csv"
    #fileName="./2.csv"
    #fileName="./need_to_get_company_id.txt"

    # how many threads you'd like to execute
    splitFileNum=3
    splitFile(fileName, pathName, splitFileNum)

    # get 營業項目編號, 營業項目描述
    url0="http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C"
    co0=ConnectionObject(url0, True)
    url1="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1"
    co1=ConnectionObject(url1, False)
    threads=[]
    for index in range(0, splitFileNum):
        threads.append(IndustryCategoryGetter(index, fileName+"_"+str(index), 'parse_category_'+str(index)+'.csv', co0, co1))

    index=0
    for index in range(0, splitFileNum):
        threads[index].start()

    index=0
    for index in range(0, splitFileNum):
        threads[index].join()
        threads[index].getOutput()


    # get 地址, 資本額, 設立日期 (以民國紀元), 解散日期 (歇業日期), 暫停開始日期(開始停業日期), 暫停結束日期 (結束停業日期 i.e. 重新開始營業日期)
    url0="http://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    co0=ConnectionObject(url0, True)
    url1="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1"
    co1=ConnectionObject(url1, False)
    
    threads=[]
    for index in range(0, splitFileNum):
        threads.append(OtherInfoGetter(index, fileName+"_"+str(index), 'parse_other_info_'+str(index)+'.csv', co0, co1))

    for index in range(0, splitFileNum):
        threads[index].start()

    for index in range(0, splitFileNum):
        threads[index].join()
        threads[index].getOutput()

    

    