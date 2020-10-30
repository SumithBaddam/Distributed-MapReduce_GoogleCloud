# Google cloud mapper node for MapReduce
import socket
import json
import ast
import sys
from map_wc import wordCount
from map_ii import invertedIndex
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

masterIpAddress = config['master']['ipAddress']
masterPort = int(config['master']['port'])

dataStoreIpAddress = config['dataStore']['ipAddress']
dataStorePort = int(config['dataStore']['port'])


class Map:
	def initiate(self):
		masterSocket = socket.socket()
		masterSocket.connect((masterIpAddress, masterPort))
		dataStoreSocket = socket.socket()
		dataStoreSocket.connect((dataStoreIpAddress, dataStorePort))
		return masterSocket, dataStoreSocket


	def fetchDataFromStore(self, dataStoreSocket, mapID):
		dataStoreSocket.send(str.encode('get input '+str(mapID)))
		data = dataStoreSocket.recv(2048).decode('utf8')
		print(data)
		return data


	def performMapTask(self, data, operation):
		if(operation == 'word_count'):
			mapOutput = wordCount(data)
		elif(operation == 'inverted_index'):
			mapOutput = invertedIndex(data)
		return mapOutput


	def writeOutputToDataStore(self, mapID, mapOutput, dataStoreSocket):
		dataStoreSocket.send(str.encode('set mapOutput '+str(mapID)+' '+str(mapOutput)))
		received = dataStoreSocket.recv(2048).decode('utf8')
		if(received == 'success'):
			print('Data from map '+str(mapID)+' is sent to KV store')
			return
		print('Data from map '+str(mapID)+' FAILED to send to KV store')
		

	def startMapTask(self):
		masterSocket, dataStoreSocket = self.initiate()
		print('Created master and data store sockets')
		# Get map ID from master
		masterSocket.send(str.encode('Mapper get ID'))
		print('Sent request to get map ID')
		a = masterSocket.recv(2048).decode('utf8')
		mapID, operation = int(a.split(' ')[0]), int(a.split(' ')[1])
		if(operation == 1):
			operation = 'word_count'
		else:
			operation = 'inverted_index'
		print('Map ID is: ', mapID)
		# Read map input data from key-value data store
		data = self.fetchDataFromStore(dataStoreSocket, mapID)
		print('Fetched data from store')
		# Perform mapper task
		mapOutput = self.performMapTask(data, operation)
		print(mapOutput)
		# Write to key-value store
		self.writeOutputToDataStore(mapID, mapOutput, dataStoreSocket)
		# Send ACK to master that map job completed
		masterSocket.send(str.encode('Mapper completed '+str(mapID)))
		print('Sent ACK of completion to master')
		# Close all sockets
		masterSocket.close()
		dataStoreSocket.close()


if __name__ == '__main__':
	mapNode = Map()
	mapNode.startMapTask()
