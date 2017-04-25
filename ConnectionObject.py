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
import requests
import traceback

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
            #print(response.text)
            if response != None and response.text.strip() != '':
                return response.json()
            else:
                return None
        except Exception as e:
            print('url:',self.url)
            print('payload:',self.payload)
            print(response.text)
            traceback.print_exc()
            return None