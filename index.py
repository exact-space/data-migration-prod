from dataMigrationImpl import dataMiragtion,os,requests,json,createConfig,config
import paho.mqtt.client as paho
from apscheduler.schedulers.background import BackgroundScheduler
# unitsId = os.environ.get("unitsId")
# if unitsId == None:
    # print("Pass in valid unitsId \n exiting...")
    # exit()
    


unitsId = (os.environ.get("UNIT_ID"))
if unitsId == None:
    print("no unitsId passed. Exiting...")
    exit()

# unitsId = ["65cb7436d91f1f00074fe652"] # qa
# unitsId = ["65cf3bf83fec960007eb619e"] # sandbox
dm = dataMiragtion(unitsId)
# dm.prodFuncHistoricData(True)
# exit()
def on_message_prod(client, userdata, msg):
    pass

def on_connect_prod(client, userdata, flags, rc):
    print("connected to prod")
    

def on_log_prod(client, userdata, obj, buff):
    print("prod_log:" + str(buff))

port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)

print("Running port", port)

client_prod= paho.Client()
client_prod.on_log = on_log_prod
client_prod.on_connect = on_connect_prod
client_prod.on_message = on_message_prod
client_prod.username_pw_set(username=config["BROKER_USERNAME"], password=config["BROKER_PASSWORD"])

print(config['BROKER_ADDRESS'])
client_prod.connect(config['BROKER_ADDRESS'], port, 60)


dm.mainFuncLiveData(client_prod)
scheduler = BackgroundScheduler()
scheduler.add_job(func=dm.mainFuncLiveData,args=[client_prod], trigger="interval", seconds=60,max_instances=1)
scheduler.start()


client_prod.loop_forever(retry_first_connection=True)
