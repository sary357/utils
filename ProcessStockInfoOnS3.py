###########################
# these python codes are for getting stock information from google
# and calculte k-value
# finally, put the result on s3 and email the report
###########################
import os
from datetime import datetime
import urllib.request 
import boto3
import botocore

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

bucket=os.environ['S3_BUCKET_NAME']
data_folder=os.environ['DATA_FOLDER']
stock_symbol=os.environ['STOCK_SYMBOL_FILE']
stock_url=os.environ['GOOGLE_STOCK_API_URL']
data_file_interval=int(os.environ['DATA_FILE_INTERVAL'])
# 
stock_info_title="unix_timestamp,yyyymmdd,close,high,low,open,volume,RSV,k"
k_value_upperbound=float(os.environ['K_VALUE_UPPERBOUND'])
k_value_lowerbound=float(os.environ['K_VALUE_LOWERBOUND'])
sender=os.environ['SENDER']
recipients=os.environ['MAIL_RECIPIENTS']

# SMTP Config
EMAIL_HOST = os.environ['EMAIL_HOST']
EMAIL_HOST_USER = os.environ['EMAIL_HOST_USER']
EMAIL_HOST_PASSWORD = os.environ['EMAIL_HOST_PASSWORD']
EMAIL_PORT = int(os.environ['EMAIL_PORT'])


def get_stock_symbol():
######################################################################
  #Return: ['stock_symbol1,(T|F)', 'stock_symbol2,(T|F)',...]:
  #  T=True: means this stock info exists in S3
  #  F=False: means this stock info does not exist in S3
######################################################################
    result_arr=[]
    is_exist='F'
    s3=boto3.resource('s3')
    try: 
        bucketObj=s3.Bucket(bucket)
        bucketObj.download_file(data_folder+'/'+stock_symbol,'/tmp/'+stock_symbol)
        
        stock_info = open('/tmp/'+stock_symbol, 'r')
        for s in stock_info:
            is_exist='F'
            for obj in bucketObj.objects.filter(Prefix=data_folder+'/'):
                if obj.key == data_folder+'/'+s.strip():
                    is_exist='T'
            result_arr.append(s.strip()+','+is_exist)
            
        return result_arr
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
            return None
        else:
            raise
    finally:
        stock_info.close()

def update_stock_info_in_s3(stock_symbol_list, input_data):
    result_arr=[]
    for s in stock_symbol_list:
        stock_info_arr=s.split(',')
        if stock_info_arr[1] == 'T':
            k=update_single_stock_info_in_s3(True, stock_info_arr[0], input_data[stock_info_arr[0]])
        else:
            k=update_single_stock_info_in_s3(False, stock_info_arr[0], input_data[stock_info_arr[0]])
        result_arr.append(stock_info_arr[0]+','+str(k))
    return result_arr

def caculate_rsv_k(arr):
    matrix_width = 6;
    matrix_height=len(arr)-1 # we have title, so the number of records will be length -1
    matrix= [[0 for x in range(matrix_width)] for y in range(matrix_height)]
    #source(tmp_arr): unix_timestamp,yyyymmdd,close,high,low,open,volume,RSV,k
    #matrix: close,high,low,open,RSV,k
    #         0     1    2    3   4  5
    indx=0
    idx=0
    for d in arr:
        if idx>0:
            tmp_str_arr=d.split(',')
            matrix[indx][0]=float(tmp_str_arr[2]) # close
            matrix[indx][1]=float(tmp_str_arr[3]) # high
            matrix[indx][2]=float(tmp_str_arr[4]) # low
            matrix[indx][3]=float(tmp_str_arr[5]) # open
            if len(tmp_str_arr)>=8: # rsv
                matrix[indx][4]=float(tmp_str_arr[7])
            else:
                matrix[indx][4]=0.0
            if len(tmp_str_arr)>=9: #daily K value
                matrix[indx][5]=float(tmp_str_arr[8])
            else:
                matrix[indx][5]=0.0 # daily K value
            indx=indx+1
        idx=1
        
    for idx in range(matrix_height):
        min_value=5000
        max_value=-1
        if idx>0 and (idx)<matrix_height and matrix[idx][4]==0.0:
            # get max value in 2 days
            if matrix[idx][1] < matrix[idx-1][1]:
                max_value=matrix[idx-1][1]
            else:
                max_value=matrix[idx][1]
                
            # get min value in 2 days
            if matrix[idx][2] > matrix[idx-1][2]:
                min_value=matrix[idx-1][2]
            else:
                min_value=matrix[idx][2]
            matrix[idx][4]=(matrix[idx][0]-min_value)/(max_value-min_value)
            
    for idx in range(matrix_height):
        if idx > 0 and matrix[idx][5]==0.0:
            if idx==1:
                matrix[idx][5]=50
            else:
                matrix[idx][5]=100.0/3*matrix[idx][4]+2/3.0*matrix[(idx-1)][5]
    #print(matrix)
    result=[]
    idx=0
    for d in arr:
        if idx==0:
            result.append(d)
        else:
            result.append(d+','+str(matrix[(idx-1)][4])+','+str(matrix[(idx-1)][5]))
        idx=idx+1
    return result
            
            
    
# stock_info_title="unix_timestamp,yyyymmdd,close,high,low,open,volume,RSV,k"
# return last_K_value
def update_single_stock_info_in_s3(is_exist_on_s3, single_stock_symbol, input_data):
    s3=boto3.resource('s3')
    
    tmp_arr=[]
    unix_timestamp_set=set()
    
    try: 
        bucketObj=s3.Bucket(bucket)
        if is_exist_on_s3:
            bucketObj.download_file(data_folder+'/'+single_stock_symbol,'/tmp/'+single_stock_symbol)
            single_stock_symbol_file = open('/tmp/'+single_stock_symbol, 'r')
            for l in single_stock_symbol_file:
                tmp_arr.append(l.strip())
                unix_timestamp_set.add(l.split(',')[0].strip())
            single_stock_symbol_file.close()
        else:
            tmp_arr.append(stock_info_title)
        
        for d in input_data:
            unix_timestamp=d.split(',')[0]
            if unix_timestamp not in unix_timestamp_set:
                tmp_arr.append(d)
                
        tmp_arr_rsv_k=caculate_rsv_k(tmp_arr)
        
        # prepate output data 
        output_data= ''
        for d in tmp_arr_rsv_k:
            output_data=output_data+d+'\n'
        last_k_value_and_date=tmp_arr_rsv_k[len(tmp_arr_rsv_k)-1].split(',')[8]+','+tmp_arr_rsv_k[len(tmp_arr_rsv_k)-1].split(',')[1]
        
        s3client=boto3.client('s3')
        # remove file on s3 
        if is_exist_on_s3:
            response=s3client.delete_object(Bucket=bucket, Key=data_folder+'/'+single_stock_symbol)

        # upload+set public
        s3client.put_object(Bucket=bucket, ContentType='text/plain', 
                            Key=data_folder+'/'+single_stock_symbol,Body=output_data, ACL='public-read')
   
        return last_k_value_and_date
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object ("+data_folder+'/'+single_stock_symbol+") does not exist.")
            return None
        else:
            raise


def download_stock_from_url(stock_symbol_list):
######################################################################
  #Return: {'stock_symbol_1': ['unix_timestamp1,yyyymmdd,close,high,low,open,volume',
  #         'unix_timestamp2,close,high,low,open,volume'....]}
######################################################################
    tmp_arr=None
    result_dic={}
    try:
        for s in stock_symbol_list:
            #print(s)
            stock_symbol=s.split(',')[0]
            destination_url=stock_url+stock_symbol
            tmp_arr=[]
            #print(destination_url)
            unix_timestamp=0
            with urllib.request.urlopen(destination_url) as f:
                response=f.read().decode('utf-8')
                data_arr=response.split('\n')
                for d in data_arr:
                    data_line=d.split(',')
                    if 'a' in d:
                        unix_timestamp=int(data_line[0].replace('a',''))
                        #print(unix_timestamp)
                        yyyymmdd=datetime.fromtimestamp(unix_timestamp).strftime('%Y%m%d')
                        tmp_arr.append(str(unix_timestamp)+","+str(yyyymmdd)+','+data_line[1]+","+data_line[2]+","+data_line[3]+","+data_line[4]+","+data_line[5])
                    elif unix_timestamp != 0 and len(d)>3:
                        interval=int(data_line[0])
                        current_unix_timestamp=unix_timestamp+(interval*data_file_interval)
                        yyyymmdd=datetime.fromtimestamp(current_unix_timestamp).strftime('%Y%m%d')
                        tmp_arr.append(str(current_unix_timestamp)+","+str(yyyymmdd)+','+data_line[1]+","+data_line[2]+","+data_line[3]+","+data_line[4]+","+data_line[5])
                if tmp_arr != None and len(tmp_arr) >0:
                    result_dic[stock_symbol]=tmp_arr
       
        return result_dic
    except Exception as e:
        print('Failed to get info from the URL:("' +stock_url + '") Or the format of API changed.' )
        raise

def notify_by_mail(mail_subject, mail_body, priority=None):
    current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = MIMEText(mail_body+'\n* This report is generated at '+current_time)
    msg['Subject'] = mail_subject
    msg['From'] = sender
    msg['To'] = recipients
    if priority != None and int(priority) >=1 and int(priority)<=5:
        msg['X-Priority'] = str(priority)
   
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.send_message(msg)
    s.quit()

def lambda_handler(event, context):
    # Step 1: list the stock symbol
    print('Step 1: get stock symbol')
    stock_symbol_arr=get_stock_symbol()
    print('Stock symbol we are going to process:')
    print(stock_symbol_arr)
    print('\n')
    
    # step 2:
    print('Step 2: download stock info from Google')
    if stock_symbol_arr != None:
        data_from_google=download_stock_from_url(stock_symbol_arr)
        #print(data_from_google)
    print('\n')
     
    # step 3:
    print('Step 3: update the data and upload to s3 and get the latest K value')
    result_k_arr=update_stock_info_in_s3(stock_symbol_arr, data_from_google)
    print('the latest K value')
    print(result_k_arr)
    print('\n')
    
    # step 4:
    # email out if 1) K value <=20
    #              2) K value >=80
    print('Step 4: generate the report and email')
    buy_stock_list=[]
    sell_stock_list=[]
    data_update_time=None
    for k in result_k_arr:
        k_value=float(k.split(',')[1])
        stock_symbol_tmp=k.split(',')[0]
        if k_value >= k_value_upperbound:
            sell_stock_list.append(stock_symbol_tmp)
        elif k_value <= k_value_lowerbound:
            buy_stock_list.append(stock_symbol_tmp)
        data_update_time=k.split(',')[2]
            
    #current_time=datetime.now().strftime('%Y/%m/%d')
    report_date_update_time=data_update_time[:4]+'/'+data_update_time[4:6]+'/'+data_update_time[6:8]
    action_str='Daily K-value analysis report ('+report_date_update_time+'): \n'
    if len(buy_stock_list) == 0 and len(sell_stock_list) ==0:
        action_str=action_str+"    No action needed. Please wait for the next trading day." + '\n'
    if len(buy_stock_list) >0:
        action_str=action_str+"    The stock symbol we recommend to buy: " + ','.join(buy_stock_list) + '\n'
    if len(sell_stock_list) > 0:
        action_str=action_str+"    The stock symbol we recommend to sell: " + ','.join(sell_stock_list) + '\n'
    print(action_str)
    if len(buy_stock_list) >0 or len(sell_stock_list)>0:
        notify_by_mail("[IMPORTANT] K-value analysis report ("+report_date_update_time+")", action_str,1)
    else:
        notify_by_mail("[No action needed] K-value analysis report ("+report_date_update_time+")",action_str)
    #s3 = boto3.resource('s3')
    #for bucket in s3.buckets.all():
    #    print(bucket.name)
    #response = s3.get_object(Bucket=bucket, Key=key)
    #content = response['Body'].read().decode('utf-8')
    #print('Checking {} at {}...'.format(SITE, event['time']))
    #print(content)
   # try:
   #     if not validate(str(urlopen(SITE).read())):
#            raise Exception('Validation failed')
#    except:
###        print('Check failed!')
#        raise
#    else:
#        print('Check passed!')
#        return event['time']
#    finally:
#        print('Check complete at {}'.format(str(datetime.now())))