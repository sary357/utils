#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
從SME資料檔案得出. 中小企業在

1. 每一個縣市分布情形 (除批發零售業是小類, 其他用產業中類分別)
source file: sme_all_industry.csv

2. 根據業務區中心的分布情形 (除批發零售業是小類, 其他用產業中類分別)
source file: sme_all_industry.csv and 新興業務區中心.xlsx

3. 核貸案件在每一個縣市分布情形 (除批發零售業是小類, 其他用產業中類分別)
source file: uniaprcust.csv

4. 核准案件在每一個業務區中心的分布情形 (除批發零售業是小類, 其他用產業中類分別)
source file: uniaprcust.csv and 新興業務區中心.xlsx
    
Created on Mon May 15 15:52:40 2017

@author: fuming.tsai
"""
import datetime
from utils import getTmpFileNamePostfix
from openpyxl import Workbook

def printDictionary(result):
# just a test.
# print the result dictionary
    print('==================================================')
    print(result)

def ingestData(key, category, resultDictionary):    
    if key in resultDictionary:
        value=resultDictionary[key]
        if category in value:
            value[category]=value[category]+1
        else:
            value[category]=1
    else:
        value={}
        value[category]=1
        resultDictionary[key]=value
def outputStatisticsNumberToExcel(reviewedDataDict, totalDataDict,titleList, tabNamPrefix,fileName):
    keySet=totalDataDict.keys()
    wb=Workbook()
    for key in keySet:
        ws=wb.create_sheet(tabNamPrefix+key)
        ws.title=tabNamPrefix+key
        tmpDic=totalDataDict[key]
        tmpKeySet=tmpDic.keys()
        indx=1
        for t in titleList:
            ws.cell(row=1,column=indx, value=t)
            indx+=1
        indx=2
        
        for k in tmpKeySet:
            formula='sum('
            ws.cell(row=indx, column=1, value=k)
            ws.cell(row=indx, column=3, value=tmpDic[k])
            #ws.cell(row=indx, column=4, value=)
            indx+=1
        formulaReview="=sum(b2:b"+str(indx-1)+")"
        formulaTotal="=sum(b2:b"+str(indx-1)+")"
        ws.cell(row=indx+1, column=1, value='全部家數')
        ws.cell(row=indx+1, column=2, value=formulaReview)
        ws.cell(row=indx+1, column=3, value=formulaTotal)
    wb.save(fileName)
    

# I: setting
path='D:/fuming.Tsai/Documents/Tools/PortableGit/projects/GitDocs/06-專案文件/05-SME授信客戶資金需求/'
smeFile='sme_all_industry.csv'
#smeFile='test20170516.csv'
smeCategoryFile='新興業務區中心.csv'
reviewResult='uniaprcust.csv'
categoryThree=['批發及零售業']
outputFileCountyDistribution='county_stat_'+getTmpFileNamePostfix()+'.xlsx'
outputFileCallCenterDistribution='call_center_stat_'+getTmpFileNamePostfix()+'.xlsx'

# II: initialize
tmpDictionary={}
countyDictionary={}
callCenterDictionary={}
statisticsCountres={}
statisticsCallCenters={}
statisticsReviewCountres={}
statisticsReviewCallCenters={}
tmpFile=getTmpFileNamePostfix()+'.csv'
categorylist=set()


# get county data and district data
countyData=open(path+'/'+smeCategoryFile, 'r', encoding='UTF-8')
for record in countyData:
    data=record.split(',')
    countyDictionary[data[0].replace('\ufeff','')]=data[2].replace('\ufeff','')
    callCenterDictionary[data[0].replace('\ufeff','')]=data[1].replace('\ufeff','')
countyData.close()

# generate statistics report
smeSourceData=open(path+'/'+smeFile, 'r', encoding = 'UTF-8')
#smeTmpData=open(path+'/'+tmpFile, 'w',  encoding = 'UTF-8')
totalSMECount=0
for record in smeSourceData:
    data=record.split(',')
    isSME=data[13]
    totalSMECount+=1
    if 'Y' in isSME or 'y' in isSME: # for SME   
        #category=data[6]
        categoryOne=data[5]
        categorylist.add(categoryOne)
        #if categoryOne in categoryThree:
        #    category=data[7]      
        dataCounty=data[9]
        
        #  Part I: get category in each county
        statisticsCounty=countyDictionary[dataCounty]
        ingestData(statisticsCounty, categoryOne,statisticsCountres )
                              
        # Part II: get category in each call center  
        callCenter=callCenterDictionary[dataCounty]
        ingestData(callCenter, categoryOne,statisticsCallCenters )
           
#smeTmpData.close()
smeSourceData.close()

smeReviewData=open(path+'/'+reviewResult, 'r', encoding='UTF-8')
idx=0
for record in smeReviewData:
    if idx > 0:
        data=record.split(',')
        category=data[6]
        categoryOne=data[5]
        if categoryOne in categoryThree:
            category=data[7]      
        dataCounty=data[2]
        
        #  Part I: get category in each county
        statisticsCounty=countyDictionary[dataCounty]
        ingestData(statisticsCounty, categoryOne,statisticsReviewCountres )
                              
        # Part II: get category in each call center  
        callCenter=callCenterDictionary[dataCounty]
        ingestData(callCenter, categoryOne,statisticsReviewCallCenters )
    idx+=1
smeReviewData.close()
#
#outputStatisticsNumberToExcel(statisticsCountres,('產業別','核貸家數','中小企業總家數'),'',path+'/'+outputFileCountyDistribution)
printDictionary(statisticsCountres)
printDictionary(statisticsCallCenters)
#printDictionary(countyDictionary)
#printDictionary(callCenterDictionary)
printDictionary(statisticsReviewCountres)
printDictionary(statisticsReviewCallCenters)
print(totalSMECount)


