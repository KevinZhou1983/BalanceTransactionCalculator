
from SqlLiteUtil import SqlLiteUtil        
import datetime
import sqlite3


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
            db.updateCustomerBalance((transactionAmount,userid))
            db.conn.commit()
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
            db.conn.rollback()
        finally:
            # 关闭游标和连接
            db.cursor.close()
            db.conn.close()




   
  

def main(): 

    # 一个事务：
    # addConsumeTransactiondetails，
    # updateChat history

    results=calculateTransactountAmountForPendingChargeRecords()
    addConsumeTransactiondetails(results)



   

    #一个事务:
    #addConsumeTransactionForChargeFee,
    #update ConsumeTransactiondetails satus
    #update balance 

    results2=getConsumeTransactionDetailsForChargeFee()
    consumeTransactionForChargeFee_db(results2)



main()