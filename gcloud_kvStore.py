# Google cloud Key-value store for MapReduce
import socket
import json
import ast
import threading
import os.path
from os import path
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
fileLock = threading.Lock()

'''
from googleapiclient import discovery
from gcp_instances import *
from oauth2client.client import GoogleCredentials
zone = config['general']['zone']
project = config['general']['project']

credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)
all_instances = list_instances(service, project, zone)
instanceNameToIP = {}
for instance in all_instances:
	instanceNameToIP[instance['name']] = instance['networkInterfaces'][0]['networkIP']

dataStoreInstanceName = config['dataStore']['instanceName']
dataStoreIpAddress = instanceNameToIP[dataStoreInstanceName]
dataStorePort = int(config['dataStore']['port'])
kvStoreFile = config['dataStore']['dataStoreFile']
'''

zone = config['general']['zone']
project = config['general']['project']

dataStoreInstanceName = config['dataStore']['instanceName']
dataStoreIpAddress = config['dataStore']['ipAddress']
dataStorePort = int(config['dataStore']['port'])
kvStoreFile = config['dataStore']['dataStoreFile']

numReducers = 1
#global kvData = {}

class KeyValueStore:
	def initiate(self):
		kvSocket = socket.socket()
		kvSocket.bind((dataStoreIpAddress, dataStorePort))
		print('Socket is bind')
		return kvSocket

	def sendRecvMessages(self, caddr, clientSocket):
		global numReducers
		while True:
			received = clientSocket.recv(2048).decode('utf8').split(' ')
			if(received[0] == 'set'):
				# Set the data into file system
				if(received[1] == 'input'):
					# This is a string
					fileLock.acquire()
					if(path.exists(kvStoreFile)):
						with open(kvStoreFile,	'r') as	outfile:
							kvData = json.load(outfile)
					else:
						kvData = {}
					key = received[2]
					value = ' '.join(received[3:])
					kvData['I_'+key] = value
					with open(kvStoreFile,	'w') as	outfile:
						json.dump(kvData, outfile)
					fileLock.release()
				elif(received[1] == 'mapOutput'):
					mapID = int(received[2])
					keyValues = ast.literal_eval(' '.join(received[3:]))
					# This is list of list
					fileLock.acquire()
					with open(kvStoreFile, 'r') as	outfile:
						kvData = json.load(outfile)
					for i in keyValues:
						try:
							# KV Store does not know number of reducers...Make mapper send it with a new format...
							kvData['M_'+str(hash(i[0])%numReducers)].append(i)
						except KeyError:
							kvData['M_'+str(hash(i[0])%numReducers)] = [i]
					
					# Write the data to a file storage
					with open(kvStoreFile,	'w') as	outfile:
						json.dump(kvData, outfile)
					fileLock.release()
				elif(received[1] == 'reduceOutput'):
					# This is a hash map
					reduceID = int(received[2])
					keyValues = ast.literal_eval(' '.join(received[3:]))
					fileLock.acquire()
					with open(kvStoreFile, 'r') as	outfile:
						kvData = json.load(outfile)

					for i in keyValues:
						kvData[i] = keyValues[i]

					with open(kvStoreFile,	'w') as	outfile:
						json.dump(kvData, outfile)
					fileLock.release()

				# Send success message
				clientSocket.send(str.encode('success'))

			elif(received[0] == 'get'):
				# Get the data from file system
				if(received[1] == 'input'):
					mapID = int(received[2])
					fileLock.acquire()
					with open(kvStoreFile, 'r') as	outfile:
						kvData = json.load(outfile)
					fileLock.release()
					clientSocket.send(str.encode(kvData['I_'+str(mapID)]))

				elif(received[1] == 'mapOutput'):
					reduceID = int(received[2])
					fileLock.acquire()
					with open(kvStoreFile, 'r') as	outfile:
						kvData = json.load(outfile)
					fileLock.release()
					clientSocket.send(str.encode(str(kvData['M_'+str(reduceID)])))
					
				elif(received[1] == 'reduceOutput'):
					fileLock.acquire()
					with open(kvStoreFile, 'r') as	outfile:
						kvData = json.load(outfile)
					fileLock.release()
					output = ''
					for key in kvData.keys():
						if('M_' not in key and 'I_' not in key):
							output = output + key + ':' + str(kvData[key]) + ', '
					clientSocket.send(str.encode(output))

			elif(received[0] == 'reducers'):
				# Number of reducers
				numReducers = int(received[1])
				# Send success message
				clientSocket.send(str.encode('ACK from KV store'))

				# Not sure whether to close the socket ?
				#clientSocket.close()
				# Delete data store file

				#break

	def kvJob(self, caddr, clientSocket):
		t = threading.Thread(target = self.sendRecvMessages, args=(caddr, clientSocket))
		t.start()
	
	def runDataStore(self, kvSocket):
		while True:
			kvSocket.listen(5)
			clientSocket, caddr = kvSocket.accept()
			print('Accepting a client')
			t = threading.Thread(target = self.kvJob, args=(caddr, clientSocket))
			t.start()


if __name__ == '__main__':
	kvStore = KeyValueStore()
	kvSocket = kvStore.initiate()
	kvStore.runDataStore(kvSocket)