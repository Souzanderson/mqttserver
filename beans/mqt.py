import sys
import paho.mqtt.client as mqtt
from beans.myq import MySql
import json
import datetime

models = {
    "acionamento_motor": {
        "idmotor": "",
        "isligado": "",
        "sentido": "",
        "velocity": ""
    },
    "cooler": {
        "idcooler": "",
        "isligado": ""
    },
    "input_a": {
        "idsensor": "",
        "value": ""
    },
    "input_b": {
        "idsensor": "",
        "value": ""        
    },
    "level": {
        "idlevel": "",
        "qtlevel": ""        
    },
    "microswitch": {
        "idlimits": "",
        "action": ""        
    },
    "motor": {
        "idmotor": "",
        "velocity": "",
        "direction": ""        
    },
    "output_a": {
        "idsensor": "",
        "value": ""        
    },
    "temperature": {
        "idsensor": "",
        "temperatura": ""        
    },
    "users": {
        "hascode": "",
        "name": "",
        "email": ""        
    }
}

class MqttBroker():
    
    def __init__(self):
        super().__init__()
        self.subscribes = [
            ("acionamento_motor",1),
            ("cooler",1),
            ("input_a",1),
            ("input_b",1),
            ("level",1),
            ("microswitch",1),
            ("motor",1),
            ("output_a",1),
            ("temperature",1),
            ("users",1)
        ]
        
        self.broker='localhost'
        self.porta = 1883
        self.keepalive = 60
        self.models = models
        
    #Callback - conexao ao broker realizada
    def on_connect(self,client, userdata, flags, rc):
        print("[STATUS] Conectado ao Broker. Resultado de conexao: "+str(rc))
        #faz subscribe automatico no topico
        client.subscribe(self.subscribes)
    
    #Callback - mensagem recebida do broker
    def on_message(self,client, userdata, msg):
        res = str(msg.payload).replace("b","").replace("'","")
        
        print("[MSG RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+res)
        
        try:
            v = self.models[msg.topic].copy()
            values = res.split("#")
            k = list(v.keys())
            for i in range(len(values)):
                v[k[i]] = values[i]
            v['dtupdate'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            print(v)
            m = MySql()
            m.insert(msg.topic,v)
            # d = {""}
        except Exception as e:
            print(e)
    
    
    def connect(self):
        print("[STATUS] Inicializando MQTT...")
        #inicializa MQTT:
        client = mqtt.Client()
        client.on_connect = self.on_connect
        client.on_message = self.on_message
 
        client.connect(self.broker, self.porta)
        client.loop_forever()
 
 