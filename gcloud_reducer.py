# Google cloud reducer node for MapReduce
import socket
import json
import ast
from reduce_wc import wordCount
from reduce_ii import invertedIndex
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

masterIpAddress = config['master']['ipAddress']
masterPort = int(config['master']['port'])

dataStoreIpAddress = config['dataStore']['ipAddress']
dataStorePort = int(config['dataStore']['port'])


class Reduce:
	def initiate(self):
		masterSocket = socket.socket()
		masterSocket.connect((masterIpAddress, masterPort))
		dataStoreSocket = socket.socket()
		dataStoreSocket.connect((dataStoreIpAddress, dataStorePort))
		return masterSocket, dataStoreSocket

	def fetchDataFromStore(self, dataStoreSocket, reduceID):
		dataStoreSocket.send(str.encode('get mapOutput '+str(reduceID)))
		data = dataStoreSocket.recv(20480).decode('utf8')
		data = ast.literal_eval(data)
		print(data)
		return data


	def performReduceTask(self, data, operation):
		if(operation == 'word_count'):
			output = wordCount(data)
		elif(operation == 'inverted_index'):
			output = invertedIndex(data)
		return output


	def writeOutputToDataStore(self, reduceID, reduceOutput, dataStoreSocket):
		print('Sending output to data store')
		dataStoreSocket.send(str.encode('set reduceOutput '+str(reduceID)+' '+str(reduceOutput)))
		received = dataStoreSocket.recv(20480).decode('utf8')
		if(received == 'success'):
			print('Data from reduce '+str(reduceID)+' is sent to KV store')
			return
		print('Data from reduce '+str(reduceID)+' FAILED to send to KV store')
		#self.writeOutputToDataStore(reduceID, mapOutput, dataStoreSocket)


	def startReduceTask(self):
		masterSocket, dataStoreSocket = self.initiate()
		print('Master and data store sockets connected')
		masterSocket.send(str.encode('Reducer get ID'))
		print('Sent request to mapper for ID')
		#reduceID = int(masterSocket.recv(20480).decode('utf8'))
		a = masterSocket.recv(2048).decode('utf8')
		reduceID, operation = int(a.split(' ')[0]), int(a.split(' ')[1])
		if(operation == 1):
			operation = 'word_count'
		else:
			operation = 'inverted_index'

		print('Reduce ID is: ', reduceID)
		# Read reduce input data from key-value data store
		data = self.fetchDataFromStore(dataStoreSocket, reduceID)
		print('Data is fetched from data store')
		# Perform reducer task
		reduceOutput = self.performReduceTask(data, operation)
		print('Reduce task is completed')
		# Write to key-value store
		self.writeOutputToDataStore(reduceID, reduceOutput, dataStoreSocket)
		# Send ACK to master that reduce job completed
		masterSocket.send(str.encode('Reducer completed '+str(reduceID)))
		print('Sent ACK completed to master')
		# Close all sockets
		masterSocket.close()
		dataStoreSocket.close()


if __name__ == '__main__':
	reduceNode = Reduce()
	reduceNode.startReduceTask()
