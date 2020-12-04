import mysql.connector
import json

host = {
    "host": "dbcalc.cfmht99spyri.us-east-2.rds.amazonaws.com",
    "port": "3306",
    "user": "admin",
    "passwd": "matheus789",
    "database": "sensors_demo"
}

class MySql():
    def __init__(self):
      self.mycursor = None
      self.mydb = None
      self.config = host
    #   self.initConf()
    
    # def initConf(self):
    #     with open('beans/config.json','r') as f:
    #         self.config = json.load(f)

    def connect(self):
        try:
            print("DB Connect...")
            self.mydb = mysql.connector.connect(
                host= self.config['host'],
                port= self.config['port'],
                user= self.config['user'],
                passwd= self.config['passwd'],
                database= self.config['database']
            )
            self.mycursor = self.mydb.cursor()
            print("Success connect!")
        except Exception as e:
            print(e)

    def reconect(self):
        try:
            self.mycursor.reset()
        except Exception as e:
            pass


    def asDic(self, cursor, first=False):
        """Retorna o resultado do cursor como um dicionário"""
        try:
            descriptions = [x[0] for x in cursor.description]
            result = cursor.fetchone()
            data = []
            while result != None:
                dic = {}
                for i in range(len(descriptions)):
                    dic[descriptions[i]] = str(result[i] if result[i] != None else "")
                data.append(dic)
                result = cursor.fetchone()
            self.mydb.commit()
            if(first):
                return data[0]
            else:
                return data
        except Exception as e:
            return {}


    def select(self,table, staments="*", where=None, groupby=None, first=False, orderby=None, dic=True, limit=1000):
        """Executa select no banco de dados (table=tabela, staments=colunas, where=dados)"""

        try:
            self.connect()
            sql = "SELECT %s FROM %s " % (staments, table)
            if where != None:
                sql += '''WHERE %s''' % (where)
            if groupby != None:
                sql += " GROUP BY %s" % (groupby)
            if orderby != None:
                sql += " ORDER BY %s" % (orderby)
            # print(sql)
            if limit != None:
                sql += " LIMIT %s" % (limit)
            cur = self.mydb.cursor()
            cur.reset()
            cur.execute(sql)
            if dic:
                return self.asDic(cur, first)
            else:
                return cur.fetchall()
        except Exception as e:
            print(e)
            return {}


    def sendSql(self,sql):
        try:
            self.connect()
            cur = self.mydb.cursor()
            cur.reset()
            cur.execute(sql)
            # print(sql)
            return self.asDic(cur, False)
        except Exception as e:
            print(e)
            return {}


    def insert(self, table, obj):
        """Insere dados em uma tabela (table=tabela, obj=dicionario de itens para inserir (key=column,value=value))"""
        try:
            self.connect()
            sql = '''INSERT INTO %s (%s) VALUES("%s")''' % (
                table, ",".join(obj.keys()), '''","'''.join(str(x).replace("'", "").replace("\"","") for x in obj.values()))
            sql = sql.replace("''", "NULL").replace("'None'", "NULL")
            # print(sql)
            self.mycursor.execute(sql)
            self.mydb.commit()
            if self.mycursor.rowcount > 0:
                return self.mycursor.lastrowid
            else:
                return None
        except Exception as e:
            print(e)
            return False


    def update(self, table, obj, conditions):
        """Atualiza os dados em uma tabela (table=tabela, obj=dicionario de itens para inserir (key=column,value=value))"""
        try:
            self.connect()
            sql = "UPDATE %s SET " % (table)
            staments = []
            for i in obj.keys():
                staments.append(i+"='"+str(obj[i])+"'")
            sql += ",".join(staments)
            sql += " where %s" % (conditions)
            print(sql)
            self.mycursor.execute(sql)
            self.mydb.commit()
            return self.mycursor.rowcount > 0
        except Exception as e:
            print(e)
            return False


    def clear(self, table, where=None):
        sql = "delete from %s" % (table)
        try:
            self.connect()
            print(sql)
            self.mycursor.execute(sql)
            self.mydb.commit()
        except Exception as e:
            print(e)


    def delete(self, table, where):
        sql = "delete from %s where %s" % (table, where)
        print(sql)
        try:
            self.connect()
            self.mycursor.execute(sql)
            self.mydb.commit()
            return self.mycursor.rowcount > 0
        except Exception as e:
            print(e)
            return False

    def jsonExample(self):
        try:
            tables = self.sendSql('show tables')
            arr = {}
            for t in tables:
                values = self.sendSql('describe %s' % t['Tables_in_sensors_demo'])
                dc = {}
                for item in values:
                    try:
                        if item['Field']!= "id" and item['Field']!= 'dtupdate':
                            dc[item['Field']]= item['Type'].decode('utf-8')
                    except:
                        dc[item['Field']]= str(item['Type'])[2:len(item['Type'])-1]
                arr[t['Tables_in_sensors_demo']] = dc
            return json.dumps(arr, indent=4)
        except Exception as e:
            print(e)
            return False
