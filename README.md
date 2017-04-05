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
### 1. Parse Excel file(sample: 對照表 - v8_10_ref_BASD_TAX_CLASSIFICATION -營業項目代碼(v8.0)與主計處(第10次修正)、財政部行業標準分類(第7次修正).xls)
- install necessary packages
```
 $ pip install xlrd
 $ pip install xlwt
```
- modify and run
  -  make sure you modified the following content
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
