import hashlib
from beans.myq import MySql

class Validator:
    
    def checkHash(self,hascode):
        try:
            m = MySql()
            user = m.select('users','*'," hascode = '%s'" % hascode,first=True)
            if(bool(user)):
                return True
            return False
        except Exception as e:
            print(e)
            return False
        
    def login(self,login,senha):
        try:
            hascode = hashlib.sha1(str(login+senha).encode()).hexdigest()
            m = MySql()
            user = m.select('users','hascode'," hascode = '%s'" % hascode,first=True)
            if(bool(user)):
                return user
            return None
        except Exception as e:
            print(e)
            return None 
    
    def getHascode(self, login, senha):
        try:
            hascode = hashlib.sha1(str(login+senha).encode()).hexdigest()
            if(bool(hascode)):
                return hascode
            return None
        except Exception as e:
            print(e)
            return None 