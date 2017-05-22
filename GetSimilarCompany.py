#/usr/bin/env python
# -*- coding: utf-8 -*-
"""
得到同一個負責人
判斷規則
1. 負責人同一個人, 但地址只有樓層差別

source file: keyman_same_sort.csv

Created on Mon May 22 12:29:21 2017

@author: fuming.tsai
"""


path='D:/fuming.Tsai/Documents/Tools/PortableGit/projects/GitDocs/06-專案文件/05-SME授信客戶資金需求/'
inputFileName='keyman_same_sort.csv'
outputFileName='keyman_same_filter.csv'
ifile=open(path+'/'+inputFileName, 'r', encoding='UTF-8')
ofile=open(path+'/'+outputFileName, 'w', encoding='UTF-8')

pre_company_addr=None
pre_company_responsible_person=None
pre_addr_no=None
pre_line=None

result=set()
for line in ifile:
    company_responsible_person=line.split('\t')[3]
    #print(company_responsible_person)
    company_addr=line.split('\t')[5]
    #print(company_addr)
    #print(line)
    if pre_line != None:
        pre_company_responsible_person=pre_line.split('\t')[3]
        pre_company_addr=pre_line.split('\t')[5]
        pre_addr_no=pre_company_addr.split('號')[0]
    if pre_company_responsible_person != None and pre_company_responsible_person in company_responsible_person:
        addr_no=company_addr.split('號')[0]
        if addr_no != None and pre_addr_no != None and addr_no in pre_addr_no:
            result.add(line)
            result.add(pre_line)
     #   else:
     #       print(pre_line)
       
    pre_line=line
        
for s in result:
    print(s)
    ofile.write(s.replace('\t',','))

ofile.close()
ifile.close()