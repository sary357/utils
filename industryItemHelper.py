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

class ConnectionObject:
    def __init__(self, connectionUrl):
        self.connectionUrl=connectionUrl
        self.restInfo=self.__parseUrlRestInfo__()
        self.schemeAndHost=self.__getSchemeAndHost__()
        
    def __parseUrlRestInfo__(self):
        urlParseResult=urlparse(self.connectionUrl) 
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
    def __getSchemeAndHost__(self):
        urlParseResult=urlparse(self.connectionUrl)       
        info=''
        if  urlParseResult.netloc != '':
            info=urlParseResult.netloc
       
        return info
    def getParseUrlRestInfo(self):
        return self.restInfo
    def getSchemeAndHost(self):
        
        return self.schemeAndHost


class OpenDataBaseParser(metaclass=ABCMeta): 
    def __init__(self, sourceFileName, outputFileName,*connectionObjects):
        self.sourceFileName=sourceFileName
        self.outputFile=outputFileName
        self.resultDisctionary={}
        self.connectionObjects=connectionObjects

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


class OtherInfoGetter(OpenDataBaseParser):
    def parse(self):
        totalLines=super().getTotalLines()
        
        try:
            index=0
            ##print(super().getSchemeAndHost(url))
            idxSuccess=0
            c=[]
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')

            cobs=self.connectionObjects
            for connectObj in cobs:
                c.append(HTTPConnection(connectObj.getSchemeAndHost()))  

            #print(restInfo)
            for line in f:
                setFlag=False
                csa='' # capital stock amount
                cl='' # company location
                csd='' # company setup date
                rkd='' # revoke date
                sbd='' # suspend beginning date
                sed='' # suspend end date

                idx=0
                bao=line.strip().replace('\ufeff','')
                if len(bao) <8:
                    bao="0"*(8-len(bao))+bao

                for connection in c:
                    if not setFlag: # connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
                        connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
                        res=connection.getresponse()
                        idx+=1
                        ##print(res.status)
                        if res.status==200:
                            data1=res.read()
                        #print(data1)
                            realData=data1.decode('UTF-8')
                        # #print(realData)
                        #     print(bao)   
                        #     print(realData)
                            if len(realData.strip())>0:
                                tempList=json.loads(realData)
                                if len(tempList)>0:
                                    # print(tempList)
                                    item=tempList[0]
                                    csd=str(item["Company_Setup_Date"]).strip()
                                    csa=str(item["Capital_Stock_Amount"]).strip()
                                    cl=str(item["Company_Location"]).strip()
                                    rkd=str(item["Revoke_App_Date"]).strip()
                                    sbd=str(item["Sus_Beg_Date"]).strip()
                                    sed=str(item["Sus_End_Date"]).strip()
                                    setFlag=True
                                   
                index=index+1
                                            # company location, company stock amount, company setup date, revoke date, suspend beginning date, suspend end date
                self.resultDisctionary[bao]=cl+","+csa+","+csd+','+rkd+','+sbd+','+sed
                print("Parsing records(OtherInfoGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
                sys.stdout.flush()
            print()
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
            for connection in c:
                connection.close()
            f.close()

class IndustryCategoryGetter(OpenDataBaseParser):

    def parse(self):
        totalLines=super().getTotalLines()
        index=0
        idxSuccess=0
        c=[]
        try:

            ##print(super().getSchemeAndHost(url))
            f=open(self.sourceFileName, 'r',  encoding='UTF-8')

            cobs=self.connectionObjects
            for connectObj in cobs:
                c.append(HTTPConnection(connectObj.getSchemeAndHost()))              

            #print(restInfo)
            for line in f:
                setFlag=False
                #self.resultDisctionary[line]=''
                idx=0
                cbItem=''
                bao=line.strip().replace('\ufeff','')
                if len(bao) <8:
                    bao="0"*(8-len(bao))+bao

                for connection in c:
                    if not setFlag:
                        connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
                        res=connection.getresponse()
                        idx+=1
                        if res.status==200:
                            data1=res.read()
                            realData=data1.decode('UTF-8')
                            if len(realData.strip())>0:
                                tempList=json.loads(realData)
                                if len(tempList) > 0:
                                    if "Cmp_Business" in tempList[0]:
                                        categoryList=tempList[0]['Cmp_Business']
                                        for category in  categoryList: 
                                            if category["Business_Seq_NO"] == "0001" and len(category["Business_Item"].strip())>0:   
                                                cbItem= str(category["Business_Item"]).strip() 
                                                setFlag=True
                                    if ("Business_Item_Old" in tempList[0]) and (len(tempList[0]['Business_Item_Old']) > 0 ):
                                        for item in tempList[0]['Business_Item_Old']:
                                            if item['Business_Seq_No'] == '1':
                                                cbItem=str(item['Business_Item'])
                                                setFlag=True
                if setFlag:
                    idxSuccess+=1                             
                                    
                index=index+1
                self.resultDisctionary[bao]=cbItem
                print("Parsing records(IndustryCategoryGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
                sys.stdout.flush()
            print("\rSuccessful items: %d/Total items: %d \r" %(idxSuccess, totalLines))
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
            for connection in c:
                connection.close()
            f.close()



if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
   
    # be sure to modify the following to reflect your file name (absolute path)
    # fileName="./SME_Closed.csv"
    fileName="./1.csv"

    # get 營業項目
    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_category_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # be sure to urlencode for each param
    url1="http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    co1=ConnectionObject(url1)
    url2="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1?%24format=json&%24filter=President_No%20eq%20"
    co2=ConnectionObject(url2)
    # p=IndustryCategoryGetter(fileName, outputFileName,co1, co2)
    # p.parse()
    # p.getOutput()

    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_otherinfo_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # # be sure to urlencode for each param
    # url1="http://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    # co1=ConnectionObject(url1)
    # url2="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1?%24format=json&%24filter=President_No%20eq%20"
    # co2=ConnectionObject(url2)
    # # get 地址, 資本額, 設立日期 (以民國紀元), 解散日期 (歇業日期), 暫停開始日期(開始停業日期), 暫停結束日期 (結束停業日期 i.e. 重新開始營業日期)
    # p=OtherInfoGetter(fileName, outputFileName, co1, co2)
    # p.parse()
    # p.getOutput()

    

    