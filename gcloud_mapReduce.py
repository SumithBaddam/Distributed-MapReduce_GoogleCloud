# Google cloud master node for MapReduce
import socket
import json
import time
import ast
from multiprocessing import Process
from googleapiclient import discovery
from gcp_instances import *
from oauth2client.client import GoogleCredentials
import configparser


class Master:
	def __init__(self):
		config = configparser.ConfigParser()
		config.read('config.ini')
		self.zone = config['general']['zone']
		self.project = config['general']['project']
		self.familyName = config['general']['familyName']

		# Building a InstanceName to IP Address translation table
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		all_instances = list_instances(service, self.project, self.zone)
		self.instanceNameToIP = {}
		for instance in all_instances:
			self.instanceNameToIP[instance['name']] = instance['networkInterfaces'][0]['networkIP']

		self.masterName = config['master']['instanceName']
		self.masterIpAddress = self.instanceNameToIP[self.masterName]
		self.masterPort = int(config['master']['port'])
		self.dataStoreInstanceName = config['dataStore']['instanceName']
		self.dataStoreIpAddress = self.instanceNameToIP[self.dataStoreInstanceName]
		self.dataStorePort = int(config['dataStore']['port'])
		self.mapNodesInstanceNamePrefix = config['map']['instanceNamePrefix']
		self.reduceNodesInstanceNamePrefix = config['reduce']['instanceNamePrefix']


	def init_cluster(self):
		# Set Master sockets
		masterSocket = socket.socket()
		masterSocket.bind((self.masterIpAddress, self.masterPort))
		# Start Key-value server
		self.startKeyValueStore()
		return masterSocket


	def readInputData(self, inputFileNames):
		data = ''
		files = inputFileNames.split(',') 
		if(len(files) < 2):
			f = open(files[0], 'r')
			data = f.read()
		else:
			for filename in files:
				f = open(filename, 'r')
				data = data + filename + ' ' + f.read()
				data = data + '#'
		print('Input data is read')
		return data.rstrip('#')


	def masterTask(self, data, masterSocket, numMappers, numReducers, operation):
		# Write input chunks to key-value store
		dataStoreSocket = self.writeInputChunksKVStore(data, numMappers, numReducers, operation)
		# Start Mapper nodes
		self.startMapperNodes(numMappers)
		# Start Master node
		self.startMasterNode(masterSocket, numMappers, numReducers, dataStoreSocket, operation)


	def startKeyValueStore(self):
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		request = service.instances().start(project=self.project , zone=self.zone, instance=self.dataStoreInstanceName)
		response = request.execute()


	def startMapperNodes(self, numMappers):
		print('Instantiating map nodes')
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		for i in range(numMappers):
			p = Process(target=self.mapStart, args = (service, self.project, self.zone, self.mapNodesInstanceNamePrefix+str(i)))
			p.start()

	def mapStart(self, service, project, zone, mapNodesInstanceName):
		all_instances = list_instances(service, self.project, self.zone)
		instance_names = []
		for instance in all_instances:
			instance_names.append(instance['name'])
		if(mapNodesInstanceName in instance_names):
			request = service.instances().start(project=self.project, zone=self.zone, instance=mapNodesInstanceName)
			response = request.execute()
		else:
			operation = create_instance(service, self.project, self.zone, mapNodesInstanceName, 'map-startup-script.sh', self.familyName)
			wait_for_operation(service, self.project, self.zone, operation['name'])
		print('Successfully launched Map instance - ' + mapNodesInstanceName)


	def stopMapperNodes(self, numMappers):
		print('Stopping map nodes')
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		for i in range(numMappers):
			p = Process(target=self.mapStop, args = (service, self.project, self.zone, self.mapNodesInstanceNamePrefix+str(i)))
			p.start()

	def mapStop(self, service, project, zone, mapNodesInstanceName):
		request = service.instances().stop(project=self.project, zone=self.zone, instance=mapNodesInstanceName)
		response = request.execute()
		print('Successfully stopped Map instance - ' + mapNodesInstanceName)
		#delete_instance(service, project, zone, mapNodesInstanceName)
		#print('Successfully deleted Map instance - ' + mapNodesInstanceName)


	def startReduceNodes(self, numReducers):
		print('Instantiating reduce nodes')
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		for i in range(numReducers):
			p = Process(target=self.reduceStart, args = (service, self.project, self.zone, self.reduceNodesInstanceNamePrefix+str(i)))
			p.start()


	def reduceStart(self, service, project, zone, reduceNodesInstanceName):
		all_instances = list_instances(service, self.project, self.zone)
		instance_names = []
		for instance in all_instances:
			instance_names.append(instance['name'])
		if(reduceNodesInstanceName in instance_names):
			request = service.instances().start(project=self.project, zone=self.zone, instance=reduceNodesInstanceName)
			response = request.execute()
		else:
			operation = create_instance(service, self.project, self.zone, reduceNodesInstanceName, 'reduce-startup-script.sh', self.familyName)
			wait_for_operation(service, self.project, self.zone, operation['name'])
		print('Successfully launched Reduce instance - ' + reduceNodesInstanceName)


	def stopReducerNodes(self, numReducers):
		print('Stopping reduce nodes')
		credentials = GoogleCredentials.get_application_default()
		service = discovery.build('compute', 'v1', credentials=credentials)
		for i in range(numReducers):
			p = Process(target=self.reduceStop, args = (service, self.project, self.zone, self.reduceNodesInstanceNamePrefix+str(i)))
			p.start()

	def reduceStop(self, service, project, zone, reduceNodesInstanceName):
		request = service.instances().stop(project=self.project, zone=self.zone, instance=reduceNodesInstanceName)
		response = request.execute()
		print('Successfully stopped Reduce instance - ' + reduceNodesInstanceName)
		#delete_instance(service, project, zone, reduceNodesInstanceName)
		#print('Successfully deleted Reduce instance - ' + reduceNodesInstanceName)


	def writeInputChunksKVStore(self, data, numMappers, numReducers, operation):
		if(operation == 1):
			operation = 'word_count'
		else:
			operation = 'inverted_index'
		# Connect to KV-store and write the input chunks
		dataStoreSocket = socket.socket()
		dataStoreSocket.connect((self.dataStoreIpAddress, self.dataStorePort))
		print('Connected to key-value store')
		dataStoreSocket.send(str.encode('reducers ' + str(numReducers)))
		received = dataStoreSocket.recv(20480).decode('utf8')
		print(received)

		if(operation == 'word_count'):
			data = data.split(' ')
			total_words = len(data)
			for i in range(numMappers):
				print('set input '+str(i)+' '+' '.join(data[(total_words*i)//numMappers : total_words*(i+1)//numMappers]))
				dataStoreSocket.send(str.encode('set input '+str(i)+' '+' '.join(data[(total_words*i)//numMappers : total_words*(i+1)//numMappers])))
				received = dataStoreSocket.recv(20480).decode('utf8')
				print('ACK received')
				if(received != 'success'):
					print('Master to KV store failed for '+ str(i))

		elif(operation == 'inverted_index'):
			data = data.split('#')
			for i in range(numMappers):
				print('set input '+str(i)+' '+ data[i])
				dataStoreSocket.send(str.encode('set input ' + str(i) + ' ' + data[i]))
				received = dataStoreSocket.recv(20480).decode('utf8')
				print('ACK received')
				if(received != 'success'):
					print('Master to KV store failed for '+ str(i))

		# Send ACK on end of set commands
		dataStoreSocket.send(str.encode('Input completed'))

		print('Input set successfully in Key-value store')
		return dataStoreSocket
		

	def fetchOutputData(self, dataStoreSocket):
		dataStoreSocket.send(str.encode('get reduceOutput'))
		received = dataStoreSocket.recv(20480).decode('utf8').strip(', ')
		print(received)

	
	def startMasterNode(self, masterSocket, numMappers, numReducers, dataStoreSocket, operation):
		count = 0
		processes = []
		while(count < numMappers):
			masterSocket.listen(5)
			clientSocket, caddr = masterSocket.accept()
			print('Master accepted client request')
			p = Process(target = self.masterJob, args=(caddr, clientSocket, numMappers, numReducers, masterSocket, dataStoreSocket, count, operation))
			p.start()
			processes.append(p)
			count += 1

		# Handling Fault-Tolerance

		start = time.time()
		for i in range(len(processes)):
			while True:
				if(processes[i].join() == None):
					# Process successfully completed within time limit
					break
				elif(time.time() - start > 300):
					# VM process has failed
					# Creating a VM
					print('Instantiating failed reduce node')
					credentials = GoogleCredentials.get_application_default()
					service = discovery.build('compute', 'v1', credentials=credentials)
					create_process = Process(target=self.mapStart, args = (service, self.project, self.zone, self.mapNodesInstanceNamePrefix+str(numMappers+i)))
					create_process.start()
					
					# Listen to the VM connection
					masterSocket.listen(1)
					clientSocket, caddr = masterSocket.accept()

					# Running the failed process 
					count += 1
					run_process = Process(target = self.masterJob, args=(caddr, clientSocket, numMappers, numReducers, masterSocket, dataStoreSocket, count, operation))
					run_process.start()
					processes.append(run_process)
					i += 1


		print('ALL MAPPERS COMPLETED')
		self.stopMapperNodes(numMappers)
		self.startReduceNodes(numReducers)

		print('ALL REDUCERS STARTING')
		count = 0
		processes = []
		while(count < numReducers):
			masterSocket.listen(5)
			clientSocket, caddr = masterSocket.accept()
			print('Master accepted client request')
			p = Process(target = self.masterJob, args=(caddr, clientSocket, numMappers, numReducers, masterSocket, dataStoreSocket, count, operation))
			p.start()
			processes.append(p)
			count += 1

		# Handling fault-tolerance for reducer VMs

		start = time.time()
		for i in range(len(processes)):
			while True:
				if(processes[i].join() == None):
					# Process successfully completed within time limit
					break
				elif(time.time() - start > 300):
					# VM process has failed
					# Creating a VM
					print('Instantiating failed reduce node')
					credentials = GoogleCredentials.get_application_default()
					service = discovery.build('compute', 'v1', credentials=credentials)
					create_process = Process(target=self.reduceStart, args = (service, self.project, self.zone, self.reduceNodesInstanceNamePrefix+str(numReducers+i)))
					create_process.start()
					
					# Listen to the VM connection
					masterSocket.listen(1)
					clientSocket, caddr = masterSocket.accept()

					# Running the failed process 
					count += 1
					run_process = Process(target = self.masterJob, args=(caddr, clientSocket, numMappers, numReducers, masterSocket, dataStoreSocket, count, operation))
					run_process.start()
					processes.append(run_process)
					i += 1
					print('TIME LIMIT EXCEEDED')
		

		print('ALL REDUCERS COMPLETED')
		self.stopReducerNodes(numReducers)
		self.fetchOutputData(dataStoreSocket)

	def masterJob(self, caddr, clientSocket, numMappers, numReducers, masterSocket, dataStoreSocket, a, operation):
		received = clientSocket.recv(20480).decode('utf8')
		#print(received)
		if(received.split(' ')[0] == "Mapper"):
			if(received.split(' ')[1] == 'get'):
				clientSocket.send(str.encode(str(a) + ' ' + str(operation)))
				status = False
				received = clientSocket.recv(20480).decode('utf8')					
				print(received)
		elif(received.split(' ')[0] == "Reducer"):
			if(received.split(' ')[1] == 'get'):
				clientSocket.send(str.encode(str(a) + ' ' + str(operation)))
				received = clientSocket.recv(20480).decode('utf8')
				print(received)
		return

	def destroy_cluster(self, masterSocket):
		masterSocket.close()


if __name__ == '__main__':
	masterObj = Master()
	masterSocket = masterObj.init_cluster()
	print('Listening')
	masterSocket.listen(5)
	clientSocket, caddr = masterSocket.accept()
	print(clientSocket, caddr)
	received = clientSocket.recv(20480).decode('utf8')
	inputFileNames, numMappers, numReducers, operation = received.split(' ')[0], int(received.split(' ')[1]), int(received.split(' ')[2]), int(received.split(' ')[3])
	print(inputFileNames, numMappers, numReducers, operation)
	data = masterObj.readInputData(inputFileNames)
	print(data)
	clientSocket.send(str.encode('success'))
	print('success')
	while True:
		received = clientSocket.recv(20480).decode('utf8')
		print('Recv: ', received)
		if(received == 'end'):
			break
		elif(received == 'run-map-reduce'):
			# Start master task - write to kv store, create mappers
			masterObj.masterTask(data, masterSocket, numMappers, numReducers, operation)
			clientSocket.send(str.encode('success'))
		elif(received == 'destroy-cluster'):
			# Destroy cluster
			masterObj.destroy_cluster(masterSocket)
			clientSocket.send(str.encode('success'))
