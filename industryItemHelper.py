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

class OpenDataBaseParser(metaclass=ABCMeta): 
    def __init__(self, sourceFileName, outputFileName,parsingUrl):
        self.sourceFileName=sourceFileName
        self.outputFile=outputFileName
        self.resultDisctionary={}
        self.parsingUrl=parsingUrl

    @abstractmethod
    def parse(self):
        pass

    def getTotalLines(self):
        try:
            num_lines = sum(1 for line in open(self.sourceFileName))
            return num_lines
        except Exception:
            raise
    
    def parseUrlRestInfo(self):
        urlParseResult=urlparse(self.parsingUrl) 
        restInfo="/"   

        
        if urlParseResult.path != '':
            restInfo=urlParseResult.path
        if urlParseResult.params!='':
            restInfo=restInfo+urlParseResult.params
        if urlParseResult.query!='':
            restInfo=restInfo+"?"+urlParseResult.query
        if  urlParseResult.fragment!='':
            restInfo=restInfo+urlParseResult.fragment
        return restInfo
    def getSchemeAndHost(self):
        urlParseResult=urlparse(self.parsingUrl)       
        info=''

        if  urlParseResult.netloc != '':
            info=urlParseResult.netloc
       
        return info

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


class OtherInfoGetter(OpenDataBaseParser):
    def parse(self):
        totalLines=super().getTotalLines()
        
        try:
            index=0
            ##print(super().getSchemeAndHost(url))
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')

            connection=HTTPConnection(super().getSchemeAndHost())
            restInfo=super().parseUrlRestInfo()

            #print(restInfo)
            for line in f:
                #self.resultDisctionary[line]=''
                bao=line.strip().replace('\ufeff','')
                
                csa='\"\"'
                cl='\"\"'
                csd='\"\"'

                connection.request("GET", restInfo+bao)
                res=connection.getresponse()
                ##print(res.status)
                if res.status==200:
                    data1=res.read()
                    #print(data1)
                    realData=data1.decode('UTF-8')
                    #print(realData)
                    #print(bao)   
                    if len(realData.strip())>0:
                        tempList=json.loads(realData)
                        for itemList in  tempList:
                            for item in itemList:                           
                                if item=="Company_Setup_Date":
                                    csd=itemList["Company_Setup_Date"]
                                   # print("Company_Setup_Date:" + itemList["Company_Setup_Date"])
                                if item=="Capital_Stock_Amount":
                                    csa=itemList["Capital_Stock_Amount"]
                                   # print("Capital_Stock_Amount:" + itemList["Capital_Stock_Amount"])
                                if item=="Company_Location":
                                    cl=itemList["Company_Location"]
                                   # print("Company_Location:" + itemList["Company_Location"])
                index=index+1
                self.resultDisctionary[bao]=cl+","+csa+","+csd
                print("Parsing records(OtherInfoGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
                sys.stdout.flush()
            print()
        except IOError as inst: 
            print("Error happened when parsing the file: " + self.sourceFileName)
            raise
        except HTTPException as e:
            print("Error happened when get the response from remote URL: " + url)
            raise
        except Exception as e:
            print("Unknown error")
            raise
        finally:
            connection.close()
            f.close()

class IndustryCategoryGetter(OpenDataBaseParser):

    def parse(self):
        totalLines=super().getTotalLines()
        index=0
        try:

            ##print(super().getSchemeAndHost(url))
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')

            connection=HTTPConnection(super().getSchemeAndHost())
            restInfo=super().parseUrlRestInfo()

            #print(restInfo)
            for line in f:
                #self.resultDisctionary[line]=''
                bao=line.strip().replace('\ufeff','')
                cbItem='\"\"'
                connection.request("GET", restInfo+bao)
                res=connection.getresponse()
                ##print(res.status)
                if res.status==200:
                    data1=res.read()
                    realData=data1.decode('UTF-8')
                    #print(realData)
                    if len(realData.strip())>0:
                        tempList=json.loads(realData)
                        for itemList in  tempList:
                            for busItems in itemList:  
                                #print(busItems)  
                                if busItems == "Cmp_Business":
                                    categoryList=itemList["Cmp_Business"]
                                    for category in   categoryList:
                                        #print(category)   
                                        if category["Business_Seq_NO"] == "0001" and len(category["Business_Item"].strip())>0:
                                               
                                            cbItem= category["Business_Item"]                                    
                                    
                index=index+1
                self.resultDisctionary[bao]=cbItem
                print("Parsing records(IndustryCategoryGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
                sys.stdout.flush()
            print()
        except IOError as inst: 
            print("Error happened when parsing the file: " + self.sourceFileName)
            raise
        except HTTPException as e:
            print("Error happened when get the response from remote URL: " + url)
            raise
        except Exception as e:
            print("Unknown error")
            raise
        finally:
            connection.close()
            f.close()


if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
   
    # be sure to modify the following to reflect your file name (absolute path)
    fileName="./SME_Closed.csv"
    #fileName="./1.csv"

    # get 營業項目
    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_category_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # be sure to urlencode for each param
    url="http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    p=IndustryCategoryGetter(fileName, outputFileName,url)
    p.parse()
    p.getOutput()

    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_otherinfo_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # be sure to urlencode for each param
    url="http://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    # get 地址, 資本額, 設立日期 (以民國紀元)
    p=OtherInfoGetter(fileName, outputFileName, url)
    p.parse()
    p.getOutput()

    