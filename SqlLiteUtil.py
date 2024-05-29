import sqlite3

class SqlLiteUtil:
    def __init__(self):
        # self.conn = sqlite3.connect('C:\\Users\\kzhou\OneDrive - GREEN DOT CORPORATION\\Documents\\GitRepo\\MyCode\\AIWEB\\DB\\OpenAI.db')
        self.conn = sqlite3.connect('/root/myai/DB/OpenAI.db')
        # self.conn = sqlite3.connect('OpenAI.db')
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        # self.cursor.execute('''CREATE TABLE IF NOT EXISTS users 
        #         (id INTEGER PRIMARY KEY, email TEXT, password INTEGER)''')
        # self.conn.commit()
   

    def insertUser(self,user, email, password,level):
        self.cursor.execute("INSERT INTO user (user,email, password,level) VALUES (?,?, ?,?)", (user,email, password,0))
        self.conn.commit()
        # self.cursor.close()
        # self.conn.close()
    

    def insertConsumeTransactionDetail(self,params):
        self.cursor.execute("INSERT INTO ConsumeTransactionDetail (userid,historyId, model,transactionAmount,transactionStatus,created,consumeTransactionDetailTypekey) VALUES (?,?, ?,?,?,?,?)", params)
        self.conn.commit()
        # self.cursor.close()
        # self.conn.close()

    def insertConsumeTransaction(self,params):
        self.cursor.execute("INSERT INTO ConsumeTransaction (userid,consumeTransactionDetailIds, transactionAmount,created) VALUES (?,?, ?,?)", params)
        # self.conn.commit()
        # self.cursor.close()
        # self.conn.close()
    def updateChatHistory(self,params):
        self.cursor.execute("update ChatHistory set chargestatus=3 where id in ({}) ".format(','.join('?'*len(params))),params)
        # self.conn.commit()
    
    def updateImageHistory(self,params):
        self.cursor.execute("update imageHistory set chargestatus=3 where id in ({}) ".format(','.join('?'*len(params))),params)
        # self.conn.commit()

    def updateConsumeTransactiondetails(self,params):        
        query = "update ConsumeTransactionDetail set transactionStatus=3 where id in ({}) ".format(','.join('?'*len(params)))
        self.cursor.execute(query, params)
        # self.cursor.execute("update ConsumeTransactionDetail set transactionStatus=3 where id in ({}) ".format(','.join('?'*len(params))),params)
        # self.conn.commit()
    
    def updateCustomerBalance(self,userid,amount,cnyRate):
        self.cursor.execute("update customer set balance=(balance-{}*{}) where userid = {}".format(amount,cnyRate,userid))
        # self.conn.commit()


    def query(self,query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # 将查询结果转换为字典列表
        result = [dict(row) for row in rows]

        # self.cursor.close()
        # self.conn.close()
        return result


    # def close(self):
    #     self.cursor.close()
    #     self.conn.close()

