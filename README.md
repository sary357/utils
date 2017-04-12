## Objective ##
- Put useful utilities functions/codes here

## Prerequisite
- Python 3 or above
- PIP

## How to develop with this packages? ##
- use virtual env (if you'd like to develop)
```
 $ source env/bin/activate
```

## How to execute?
#### Package I: parse Excel file(sample: 對照表 - v8_10_ref_BASD_TAX_CLASSIFICATION -營業項目代碼(v8.0)與主計處(第10次修正)、財政部行業標準分類(第7次修正).xls)
- install necessary packages
```
 $ pip install xlrd
 $ pip install xlwt
```
- modify the file: industryMappingParse.py
  -  make sure you modify the following content
```
   
if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()

    # be sure to modify the following to reflect your file name (absolute path)
    fileName="./對照表 - v8_10_ref_BASD_TAX_CLASSIFICATION -營業項目代碼(v8.0)與主計處(第10次修正)、財政部行業標準分類(第7次修正).xls"
    # if you'd like to have your own output file name, please modify the following
    outputFileName="./parser_"+today.strftime("%Y%m%d%H%M%S_%s")+".xls"

    main(fileName, outputFileName)
```
- run
```
  $ python industryMappingParse.py
```
- check whether there is any exception message. if not, please check the output file

#### Package II: parse CSV file (sample: SME_closed.csv)
- modify the file: industryItemHelper.py. Make sure you modify the following
```

if __name__ == '__main__':
    sys.stdout = TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace') 
    today=datetime.now()
   
    # be sure to modify the following to reflect your file name (absolute path)
    fileName="./SME_Closed.csv"
    # fileName="./1.csv"

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
    # get 地址, 資本額, 設立日期 (以民國紀元), 解散日期 (歇業日期), 暫停開始日期(開始停業日期), 暫停結束日期 (結束停業日期 i.e. 重新開始營業日期)
    p=OtherInfoGetter(fileName, outputFileName, url)
    p.parse()
    p.getOutput()

```
- run
```
 $ python industryItemHelper.py
```
- check whether there is any exception message. if not, please check the output file
