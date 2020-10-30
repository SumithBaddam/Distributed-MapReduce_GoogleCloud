# Test cases for Google cloud compute engine services

from googleapiclient import discovery
from gcp_instances import *
from oauth2client.client import GoogleCredentials
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

zone = config['general']['zone']
project = config['general']['project']
familyName = config['general']['familyName']

credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)

# Creates a VM if it does not exists and starts it if it exists
def startInstance(service, project, zone, instanceName):
	all_instances = list_instances(service, project, zone)
	instance_names = []
	for instance in all_instances:
		instance_names.append(instance['name'])
	if(instanceName in instance_names):
		request = service.instances().start(project=project, zone=zone, instance=instanceName)
		response = request.execute()
	else:
		operation = create_instance(service, project, zone, instanceName, 'map-startup-script.sh', familyName)
		wait_for_operation(service, project, zone, operation['name'])
	print('Successfully launched Map instance - ' + instanceName)


# Stops a VM
def stopInstance(service, project, zone, instanceName):
	request = service.instances().stop(project=project, zone=zone, instance=instanceName)
	response = request.execute()
	print('Successfully stopped Map instance - ' + instanceName)

# Deletes a VM
def deleteInstance(service, project, zone, instanceName):
	delete_instance(service, project, zone, instanceName)
	print('Successfully deleted Map instance - ' + instanceName)


# Builds intance name to ip-address translation table
def buildInstanceNameIPAddressTranslationTable(service, project, zone):
	all_instances = list_instances(service, project, zone)
	instanceNameToIP = {}
	for instance in all_instances:
		instanceNameToIP[instance['name']] = instance['networkInterfaces'][0]['networkIP']


# Comment and uncomment below code to run each process and test

startInstance(service, project, zone, 'sample-test-instance')
stopInstance(service, project, zone, 'sample-test-instance')
deleteInstance(service, project, zone, 'sample-test-instance')
buildInstanceNameIPAddressTranslationTable(service, project, zone)