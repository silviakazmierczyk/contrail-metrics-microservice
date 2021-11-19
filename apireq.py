# ULTIMO FUNCIONAL 26.0

import requests
from kafka import KafkaProducer
from time import sleep
import json
import os.path
import logging
from logging.handlers import SysLogHandler
import confuse

logger = logging.getLogger('contrailmetrics2kafka')

#Send a get request, get a response object
url='https://developer.nrel.gov/api/alt-fuel-stations/v1.json?fuel_type=E85,ELEC&state=CA&limit=2&api_key=07vBg1LlPomOR1xvxtW6Rs91MPTtPyO1Ztyzrx9r'

#########################################################################################
# Classes
#########################################################################################
class Config(type):
	"""
		Config file interface
		see: https://confuse.readthedocs.io/en/latest/
	"""
	c = confuse.Configuration("contrailmetrics2kafka", __name__, read=False)
	_defaults = {
        'analytics': {
			'forward_method': "kafka",
            'BROKER_BOOTSTRAP_SERVER': 'my-cluster-kafka-bootstrap.openshift-operators.svc.cluster.local:9092',
            'PRODUCER_TOPIC': 'contrail-metrics'
		},
        'logging': {
			'level': confuse.Choice(choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO"),
			'formater': "%(asctime)-15s:%(levelname)s:%(name)s:%(funcName)s:%(message)s",
			'use_syslog': confuse.Choice(choices=[True, False], default=False),
			'syslog_address': "/dev/log"
		}
    }

	@staticmethod
	def load_file(filename):
		try:
			Config.c.set_file(filename)      # Load the file
			Config.c.add(Config._defaults)   # Load defaults from template
		except confuse.ConfigError as e:
			raise Exception ("Configuration error: %s" % e)

#########################################################################################
# Functions
#########################################################################################

#%%
def receiveMessage():
    result=requests.get(url).json()
    return result
#%%
def sendMessage(result, prod):
    PRODUCER_TOPIC = Config.c['analytics']['PRODUCER_TOPIC'].get()
	
    while True:
    	sleep(5)
    	prod.send(PRODUCER_TOPIC, json.dumps(result).encode('utf-8'))
    	print(result)
    	print(prod)

def main_loop():
	BROKER = Config.c['analytics']['BROKER_BOOTSTRAP_SERVER'].get()
	
    
	prod = KafkaProducer(bootstrap_servers=BROKER)
	result = receiveMessage()
	sendMessage(result, prod)
	

def start_app():
    # Set log defaults
	#
	logger.setLevel("INFO")
	logging.basicConfig(format="%(asctime)-15s:%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    # Define config file
    #
	config_filename = os.environ.get('CONTRAILMETRICS2KAFKA_CONFIG')

    # Load config file
	#
	try:
		if config_filename is None:
		    logger.warning("Environment variable CONTRAILMETRICS2KAFKA_CONFIG should be defined. using file /contrailmetrics2kafka.yml")
		    config_filename="/contrailmetrics2kafka.yml"
		else:
			logger.info(f"Using config file: {config_filename}") #2021-11-19 14:55:47,292:INFO:contrailmetrics2kafka:start_app:Using config file: /contrailmetrics2kafka.yml
		if not os.path.exists(config_filename):
			raise Exception("Configuration file %s does not exists" % config_filename)
		if not os.path.isfile(config_filename):
			raise Exception("Expected configuration file %s isn't a file" % config_filename)
		Config.load_file(config_filename)
	except Exception as error:
		logger.error(error)
		exit(1)

    # Redefine logging from configuration
	#
	logging.basicConfig(format=Config.c['logging']['format'].get())
	logger.setLevel(Config.c['logging']['level'].get())
	if Config.c['logging']['use_syslog'].get():
		try:
			logger.addHandler(SysLogHandler(address=Config.c['logging']['syslog_address'].get()))
			logger.addHandler(SysLogHandler(address=Config().logging.syslog_address))
		except Exception as x:
			logger.warning("Cant use syslog: "+str(x))
            
    # Define message senders
	#
	logger.info("Start") #2021-11-19 14:55:47,294:INFO:contrailmetrics2kafka:start_app:Start

	main_loop()  # Loop forever


#########################################################################################
# MAIN
#########################################################################################
#%%
# Initializes the producer object named prod and sends a message to it
def main(): 
    start_app()

if __name__=="__main__":
    #x = sys.argv[1]
    main()



