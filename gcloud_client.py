# Google cloud client for MapReduce
import socket
import json
import time
import ast
from multiprocessing import Process
from gcloud_mapReduce import *
from googleapiclient import discovery
from gcp_instances import *
from oauth2client.client import GoogleCredentials
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

zone = config['general']['zone']
project = config['general']['project']
familyName = config['general']['familyName']
masterInstanceName = config['master']['instanceName']
masterPort = int(config['master']['port'])

def main():
	# Fetch inputs from user
	operation = int(input('Enter operation:\n1.Word count \n2.Inverted index\n'))
	inputFileNames = input('Enter file names: ')
	numMappers = int(input('Enter number of mappers: '))
	numReducers = int(input('Enter number of Reducers: '))
	start = time.time()
	p = Process(target = startProcess, args=(inputFileNames, numMappers, numReducers, operation))
	p.start()
	p.join()
	print('Total time: ', str(time.time()-start) + ' seconds')


# Fetch the external IP of the Master Instance from instance name
def getIPFromInstanceName(masterInstanceName):
	credentials = GoogleCredentials.get_application_default()
	service = discovery.build('compute', 'v1', credentials=credentials)
	all_instances = list_instances(service, project, zone)
	instanceNameToIP = {}
	for instance in all_instances:
		instanceNameToIP[instance['name']] = instance['networkInterfaces'][0]['accessConfigs'][0]

	return instanceNameToIP[masterInstanceName]['natIP']



def startProcess(inputFileNames, numMappers, numReducers, operation):
	masterIPAddress = getIPFromInstanceName(masterInstanceName)
	#print(masterIPAddress)
	masterSocket = socket.socket()
	masterSocket.connect((masterIPAddress, masterPort))
	masterSocket.send(str.encode(inputFileNames + ' ' + str(numMappers) + ' '+str(numReducers) + ' ' + str(operation)))
	received = masterSocket.recv(20480).decode('utf8')
	if(received == 'success'):
		masterSocket.send(str.encode('run-map-reduce'))
		received = masterSocket.recv(20480).decode('utf8')
	if(received == 'success'):
		masterSocket.send(str.encode('destroy-cluster'))
		received = masterSocket.recv(20480).decode('utf8')
	if(received == 'success'):
		masterSocket.send(str.encode('end'))
		print('MAP REDUCE JOB COMPLETE')


if __name__ == '__main__':
	main()