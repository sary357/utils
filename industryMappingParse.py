#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
用來處理檔案:
對照表 - 營業項目代碼與主計處、財政部行業標準分類
在這邊檔案為
對照表 - v8_10_ref_BASD_TAX_CLASSIFICATION -營業項目代碼(v8.0)與主計處(第10次修正)、財政部行業標準分類(第7次修正)

P.S: encodoing in env must be UTF-8
"""
import sys
from pathlib import Path
from datetime import datetime
from io import TextIOWrapper
import xlrd
import re
import xlwt


class IndustryMappingParser:

    def __init__(self,fileName):
        self.setFileName(fileName)
        self.__nrows=-1
        self.__ncolumns=-1
        self.__content=[]
        
    def getFileName(self):
        return self.filename  

    def setFileName(self, fileName):
        self.filename=fileName

    def parse(self):
        print("Processing: "+self.getFileName())
        try:
            excelFileData = xlrd.open_workbook(self.getFileName())
            table=excelFileData.sheets()[0]
            self.__nrows=table.nrows
            self.__ncolumns=table.ncols

            print("We're processing the file with rows: "+str(self.__nrows)+", and columns: "+str(self.__ncolumns))

            
            for i in range(0, self.__nrows):
                self.__content.append(table.row_values(i))


            for i in range(0, self.__nrows):
                if  self.__content[i][3]!= None and str(self.__content[i][3]).strip()!='' and re.match('^[a-z]|^[A-Z]|^[0-9]', str(self.__content[i][3]).strip()):
                    self.__content[i][0]=self.__content[i][3][0:1]
                    self.__content[i][1]=self.__content[i][3][0:2]
                    self.__content[i][2]=self.__content[i][3][0:4]
                if  self.__content[i][2]!= None and str(self.__content[i][2]).strip()!='' and re.match('^[a-z]|^[A-Z]|^[0-9]', str(self.__content[i][2]).strip()):
                    self.__content[i][0]=self.__content[i][2][0:1]
                    self.__content[i][1]=self.__content[i][2][0:2]
                if  self.__content[i][1]!= None and str(self.__content[i][1]).strip()!='' and re.match('^[a-z]|^[A-Z]|^[0-9]', str(self.__content[i][1]).strip()):
                    self.__content[i][0]=self.__content[i][1][0:1]

            for i in range(0, self.__nrows):
                if (self.__content[i][9] != None and str(self.__content[i][9]).strip()!='') or (self.__content[i][14] != None or str(self.__content[i][14]).strip()!=''):
                    if self.__content[i][0]== None or str(self.__content[i][0]).strip()=='':
                        if(i>4):
                            self.__content[i][0]=self.__content[i-1][0]
                            self.__content[i][1]=self.__content[i-1][1]
                            self.__content[i][2]=self.__content[i-1][2]
                            self.__content[i][3]=self.__content[i-1][3]
                            self.__content[i][4]=self.__content[i-1][4]
          
        except Exception as inst:
            print('Failed to parse the file:' + self.getFileName())
            raise
            

    def outputToFile(self, outputFileName):
        if self.__ncolumns>-1 and self.__nrows>-1:
            print("Dump output to the file: "+outputFileName)
            #for i in range(0, self.__nrows):
            #   print(self.__content[i])
            style = xlwt.XFStyle() 
            font = xlwt.Font() 
            font.name = '新細明體' # for mac @@
            style.font = font #
            workbook=xlwt.Workbook()
            sheet = workbook.add_sheet('result')
            for i in range(0, self.__nrows):
                for j in range(0, self.__ncolumns):
                    sheet.write(i,j,self.__content[i][j], style)
            workbook.save(outputFileName)

                
                    

def main(fileName,output):
    f=Path(fileName)
    if f.is_file:
        parser=IndustryMappingParser(fileName)
        parser.parse()
        parser.outputToFile(output)


if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()

    # be sure to modify the following to reflect your file name (absolute path)
    fileName="./對照表 - v8_10_ref_BASD_TAX_CLASSIFICATION -營業項目代碼(v8.0)與主計處(第10次修正)、財政部行業標準分類(第7次修正).xls"
    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_"+today.strftime("%Y%m%d%H%M%S_%s")+".xls"

    main(fileName, outputFileName)

    

