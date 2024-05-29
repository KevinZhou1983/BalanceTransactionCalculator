
from SqlLiteUtil import SqlLiteUtil        
import datetime
import sqlite3
import requests
import logging
logger = logging.getLogger('log')
logging.basicConfig(level=logging.INFO)
def calculateTransactountAmountForPendingChargeRecords():
    db = SqlLiteUtil()
    query="""
                
            select ch.userid,ch.id as historyId,ch.Model,
            (p.inputPrice/1000000)*ch.PromptTokens+(p.outputPrice/1000000)*ch.CompletionTokens  as transactionAmount
            ,1 as transactionStatus,ch.created,1 as consumeTransactionDetailTypekey
            from chathistory ch
            join chatprice p on ch.model = p.model
            where ch.chargestatus=1
            union

            select ch.userid,ch.id as historyId,ch.Model,
            p.price as transactionAmount,
            1 as transactionStatus,
            ch.created,2 as consumeTransactionDetailTypekey
            from imagehistory ch
            join imageprice p on ch.model = p.model 
            and LOWER(ch.quality) = LOWER(p.quality ) 
            and Trim(ch.resolution) =TRIM(p.resolution)
            where ch.chargestatus=1 ;


        """
    rows=db.query(query)  
    return rows

def addConsumeTransactiondetails(rows):         
    for row in rows:      
        current_time = datetime.datetime.now()
        timestamp = int(current_time.timestamp())

        userid=row['UserId']
        historyId=row['historyId']
        model=row['Model']
        transactionAmount=row['transactionAmount']
        created=timestamp
        consumeTransactiondetailTypekey=row['consumeTransactionDetailTypekey']
        params=(userid,historyId,model,transactionAmount,1,created,consumeTransactiondetailTypekey)               
        try:
            db = SqlLiteUtil()
            db.cursor.execute("BEGIN;")
            db.insertConsumeTransactionDetail(params)
            if consumeTransactiondetailTypekey==1:
                db.updateChatHistory((historyId,))
            else:
                db.updateImageHistory((historyId,))                         
            db.conn.commit()
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
            db.conn.rollback()
        finally:
            # 关闭游标和连接
            db.cursor.close()
            db.conn.close()

def getConsumeTransactionDetailsForChargeFee():
    db = SqlLiteUtil()
    query="""
                SELECT userid, 
        GROUP_CONCAT(id) as consumeTransactionDetailIds , 
        SUM(transactionAmount) as transactionAmount
        FROM ConsumeTransactionDetail 
        where transactionStatus=1         
        GROUP BY userid
        having SUM(transactionAmount) > 0.015 
          limit 10;
        """
    rows=db.query(query)
    return rows



def consumeTransactionForChargeFee_db(rows):    
    for row in rows:   
        db = SqlLiteUtil()   
        current_time = datetime.datetime.now()
        timestamp = int(current_time.timestamp())

        userid=row['userId']
        consumeTransactionDetailIds=row['consumeTransactionDetailIds']        
        transactionAmount=row['transactionAmount']
        created=timestamp
        params=(userid,consumeTransactionDetailIds,transactionAmount,created)
        params2=consumeTransactionDetailIds.split(',')        
        try:
            db.cursor.execute("BEGIN;")
            db.insertConsumeTransaction(params)
            db.updateConsumeTransactiondetails(params2)
            cnyRates=getCNYRates(userid)
            db.updateCustomerBalance(userid,transactionAmount,cnyRates)
            db.conn.commit()
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
            db.conn.rollback()
        finally:
            # 关闭游标和连接
            db.cursor.close()
            db.conn.close()


def getlevel(userid):
    db = SqlLiteUtil()
    query=f"""
            select level from user where id={userid}
        """
    rows=db.query(query)
    return rows[0]['level']

def getCNYRates(userid):
    level=0
    level=getlevel(userid)
    # 替换为您的API密钥
    api_key = '33af2eb3a7a80e1a2366ab18'
    # 您可以选择任何一个基准货币
    base_currency = 'USD'

    # 构建请求URL
    url = f'https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}'
  

    # 发送请求
    response = requests.get(url,verify=False)

    # 检查响应状态码
    if response.status_code == 200:
        # 解析JSON数据
        data = response.json()
        # 输出汇率信息
        rate=float(data['conversion_rates']['CNY'])
        logging.info(f'1 {base_currency} = {rate} CNY')        
    else:
        result="Failed to get exchange rates. Status code:{}".format(response.status_code)
        logging.error(result)
        rate=7.9
        return rate
    if level==0:
        rate= rate*1.3
    elif level==1:
        rate= rate*1.2
    elif level==2:
        rate= rate*1.1
    else:
        rate= rate*1
    return rate
  

def main(): 
    # db = SqlLiteUtil()  
    # cnyRates=getCNYRates(17)
    # db.cursor.execute("BEGIN;")
    # db.updateCustomerBalance(17,1,cnyRates)
    # db.conn.commit()
    # 一个事务：
    # addConsumeTransactiondetails，
    # updateChat history

    results=calculateTransactountAmountForPendingChargeRecords()
    addConsumeTransactiondetails(results)



   

    #一个事务:
    #addConsumeTransactionForChargeFee,
    #update ConsumeTransactiondetails satus
    # #update balance 

    results2=getConsumeTransactionDetailsForChargeFee()
    consumeTransactionForChargeFee_db(results2)



main()