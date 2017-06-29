#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
用來從統一編號得到公司名稱
所使用的 URL: 
1. http://lasai.org/od/data/api/9D17AE0D-09B5-4732-A8F4-81ADED04B679?%24format=json&%24filter=Business_Accounting_NO%20eq%20統一編號
2. http://lasai.org/od/data/api/855A3C87-003A-4930-AA4B-2F4130D713DC?%24format=json&%24filter=President_No%20eq%2010統一編號

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
import threading



# class ConnectionObject:
#     def __init__(self, connectionUrl):
#         self.connectionUrl=connectionUrl
#         self.restInfo=self.__parseUrlRestInfo__()
#         self.schemeAndHost=self.__getSchemeAndHost__()
        
#     def __parseUrlRestInfo__(self):
#         urlParseResult=urlparse(self.connectionUrl) 
#         restInfo="/"   

        
#         if urlParseResult.path != '':
#             restInfo=urlParseResult.path
#         if urlParseResult.params!='':
#             restInfo=restInfo+urlParseResult.params
#         if urlParseResult.query!='':
#             restInfo=restInfo+"?"+urlParseResult.query
#         if  urlParseResult.fragment!='':
#             restInfo=restInfo+urlParseResult.fragment
#         return restInfo
#     def __getSchemeAndHost__(self):
#         urlParseResult=urlparse(self.connectionUrl)       
#         info=''
#         if  urlParseResult.netloc != '':
#             info=urlParseResult.netloc
       
#         return info
#     def getParseUrlRestInfo(self):
#         return self.restInfo
#     def getSchemeAndHost(self):
        
#         return self.schemeAndHost


class IndustryNameGetter(OpenDataBaseParser, threading.Thread):
    def __init__(self, sourceFileName, outputFileName,*connectionObjects):
        threading.Thread.__init__(self)
        self.sourceFileName=sourceFileName
        self.outputFileName=outputFileName
        


# class OpenDataBaseParser(metaclass=ABCMeta): 
#     def __init__(self, sourceFileName, outputFileName,*connectionObjects):
#         self.sourceFileName=sourceFileName
#         self.outputFile=outputFileName
#         self.resultDisctionary={}
#         self.connectionObjects=connectionObjects

#     @abstractmethod
#     def parse(self):
#         pass

#     def getTotalLines(self):
#         try:
#             num_lines = sum(1 for line in open(self.sourceFileName))
#             return num_lines
#         except Exception:
#             raise

#     def getOutput(self):
#         print("Dump output file: %s"%(self.outputFile,))
#         keyList=sorted(self.resultDisctionary)
#         try:
#             f=open(self.outputFile, mode='w', encoding="UTF-8")
#             for key in keyList:
#                 #print("%s,%s" %(key, self.resultDisctionary[key]))
#                 f.write("%s,%s\n" %(key, self.resultDisctionary[key]))
#         except Exception:
#             print("Error happened when dumping output")
#             raise
#         finally:
#             f.close()


# class OtherInfoGetter(OpenDataBaseParser):
#     def parse(self):
#         totalLines=super().getTotalLines()
        
#         try:
#             index=0
#             ##print(super().getSchemeAndHost(url))
#             idxSuccess=0
#             c=[]
#             f=open(self.sourceFileName, 'r',  encoding='UTF-8')
#             ofile=open(self.outputFile, mode='w', encoding="UTF-8")

#             cobs=self.connectionObjects
#             for connectObj in cobs:
#                 c.append(HTTPConnection(connectObj.getSchemeAndHost()))  

#             #print(restInfo)
#             for line in f:
#                 setFlag=False
#                 csa='' # capital stock amount
#                 cl='' # company location
#                 csd='' # company setup date
#                 rkd='' # revoke date
#                 sbd='' # suspend beginning date
#                 sed='' # suspend end date

#                 idx=0
#                 bao=line.strip().replace('\ufeff','')
#                 if len(bao) <8:
#                     bao="0"*(8-len(bao))+bao

#                 for connection in c:
#                     if not setFlag: # connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
                        
#                         try:
#                             sleep(0.5)
#                             connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
#                             res=connection.getresponse()
                            
#                             ##print(res.status)
#                             if res.status==200:
#                                 data1=res.read()
#                             #print(data1)
#                                 realData=data1.decode('UTF-8')
#                             # #print(realData)
#                             #     print(bao)   
#                             #     print(realData)
#                                 if len(realData.strip())>0:
#                                     tempList=json.loads(realData)
#                                     if len(tempList)>0:
#                                         # print(tempList)
#                                         item=tempList[0]
#                                         if "Company_Setup_Date" in item:
#                                             csd=str(item["Company_Setup_Date"]).strip()
#                                             setFlag=True
#                                         if "Capital_Stock_Amount" in item:
#                                             csa=str(item["Capital_Stock_Amount"]).strip()
#                                             setFlag=True
#                                         if "Company_Location" in item:
#                                             cl=str(item["Company_Location"]).strip()
#                                             setFlag=True
#                                         if "Business_address" in item:
#                                             cl=str(item["Business_address"]).strip()
#                                             setFlag=True
#                                         if "Revoke_App_Date" in item:
#                                             rkd=str(item["Revoke_App_Date"]).strip()
#                                             setFlag=True
#                                         if "Sus_Beg_Date" in item:
#                                             sbd=str(item["Sus_Beg_Date"]).strip()
#                                             setFlag=True
#                                         if "Sus_End_Date" in item:
#                                             sbd=str(item["Sus_End_Date"]).strip()
#                                             setFlag=True
#                         except Exception as e:
#                             print("Error happened")
#                             traceback.print_exc()
#                         finally:
#                             idx+=1
                 

#                 index=index+1
#                                             # company location, company stock amount, company setup date, revoke date, suspend beginning date, suspend end date
#                 #self.resultDisctionary[bao]=cl+","+csa+","+csd+','+rkd+','+sbd+','+sed
#                 of.write("%s,%s\n" %(bao, cl+","+csa+","+csd+','+rkd+','+sbd+','+sed))
#                 if setFlag:
#                     idxSuccess+=1  
#                 print("Parsing records(OtherInfoGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
#                 sys.stdout.flush()
#             print("\rSuccessful items: %d/Total items: %d \r" %(idxSuccess, totalLines))
#         except IOError as inst: 
#             print("Error happened when parsing the file: " + self.sourceFileName)
#             raise
#         except HTTPException as e:
#             print("Error happened when get the response from remote URL")
#             raise
#         except Exception as e:
#             print("Unknown error")
#             raise
#         finally:
#             for connection in c:
#                 connection.close()
#             f.close()
#             ofile.close()
#     def getOutput(self):
#         print("Dump output file: %s"%(self.outputFile,))
        

# class IndustryCategoryGetter(OpenDataBaseParser):

#     def parse(self):
#         totalLines=super().getTotalLines()
#         index=0
#         idxSuccess=0
#         c=[]
#         try:

#             ##print(super().getSchemeAndHost(url))
#             f=open(self.sourceFileName, 'r',  encoding='UTF-8')

#             cobs=self.connectionObjects
#             for connectObj in cobs:
#                 c.append(HTTPConnection(connectObj.getSchemeAndHost()))              

#             #print(restInfo)
#             for line in f:
#                 setFlag=False
#                 #self.resultDisctionary[line]=''
#                 idx=0
#                 cbItem=''
#                 bao=line.strip().replace('\ufeff','')
#                 if len(bao) <8:
#                     bao="0"*(8-len(bao))+bao

#                 for connection in c:
#                     if not setFlag:
#                         try:
#                             sleep(0.5)
#                             connection.request("GET", self.connectionObjects[idx].getParseUrlRestInfo()+bao)
#                             #print(self.connectionObjects[idx].getParseUrlRestInfo()+bao)
#                             res=connection.getresponse()
#                             if res.status==200:
#                                 data1=res.read()
#                                 realData=data1.decode('UTF-8')
#                                 if len(realData.strip())>0:
#                                     tempList=json.loads(realData)
#                                     #print(realData)
#                                     if len(tempList) > 0:
#                                         if "Cmp_Business" in tempList[0]:
#                                             categoryList=tempList[0]['Cmp_Business']
#                                             for category in  categoryList: 
#                                                 if category["Business_Seq_NO"] == "0001" :   
#                                                     cbItem= str(category["Business_Item"]).strip()+","
                                                    
#                                                     cbItem= cbItem+str(category["Business_Item_Desc"]).strip() 
                                                    
#                                                     setFlag=True
#                                         if ("Business_Item_Old" in tempList[0]) and (len(tempList[0]['Business_Item_Old']) > 0 ):
#                                             for item in tempList[0]['Business_Item_Old']:
#                                                 if item['Business_Seq_No'] == '1':
#                                                     cbItem=str(item['Business_Item']).strip()+","
#                                                     cbItem=cbItem+str(item['Business_Item_Desc']).strip()
                                                   
#                                                     setFlag=True
#                         except Exception as e:
#                             print("Error happened")
#                             traceback.print_exc()
#                         finally:
#                             idx+=1
#                 if setFlag:
#                     idxSuccess+=1                             
                                    
#                 index=index+1
#                 self.resultDisctionary[bao]=cbItem.replace('\n', '')
#                 print("Parsing records(IndustryCategoryGetter): (%d/%d)\r" %(index, totalLines), end='', flush=True)
#                 sys.stdout.flush()
#             print("\rSuccessful items: %d/Total items: %d \r" %(idxSuccess, totalLines))
#         except IOError as inst: 
#             print("Error happened when parsing the file: " + self.sourceFileName)
#             raise
#         except HTTPException as e:
#             print("Error happened when get the response from remote URL")
#             raise
#         except Exception as e:
#             print("Unknown error")
#             raise
#         finally:
#             for connection in c:
#                 connection.close()
#             f.close()



if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
    
    # the path of your source file and destination file
    pathName="./"

   
    # be sure to modify the following to reflect your file name (absolute path)
    fileName=pathName+"./SME_Closed.csv"
    #fileName="./1.csv"
    #fileName="./2.csv"

    # 從統編得到公司 or 商號名稱
    # if you'd like to have your own output file name, please modify the following
    # outputFileName=pathName+"./parser_category_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # # be sure to urlencode for each param
    # url1="http://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    # co1=ConnectionObject(url1)
    # url2="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1?%24format=json&%24filter=President_No%20eq%20"
    # co2=ConnectionObject(url2)
    # p=IndustryCategoryGetter(fileName, outputFileName,co1, co2)
    # p.parse()
    # p.getOutput()

    # # if you'd like to have your own output file name, please modify the following
    outputFileName=pathName+"./parser_otherinfo_"+today.strftime("%Y%m%d%H%M%S_%s")+".csv"
    # # be sure to urlencode for each param
    url1="http://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?%24format=json&%24filter=Business_Accounting_NO%20eq%20"
    co1=ConnectionObject(url1)
    url2="http://lasai.org/od/data/api/426D5542-5F05-43EB-83F9-F1300F14E1F1?%24format=json&%24filter=President_No%20eq%20"
    co2=ConnectionObject(url2)
    # get 地址, 資本額, 設立日期 (以民國紀元), 解散日期 (歇業日期), 暫停開始日期(開始停業日期), 暫停結束日期 (結束停業日期 i.e. 重新開始營業日期)
    p=OtherInfoGetter(fileName, outputFileName, co1, co2)
    p.parse()
    p.getOutput()

    

