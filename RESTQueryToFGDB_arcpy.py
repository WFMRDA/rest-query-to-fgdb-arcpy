#REST Service to File Geodatabase
#
#Who:
#Andrew D. Bailey, Data Manager
#Department of Interior - Office of Wildland Fire
#Wildland Fire Management Research, Development, and Application
#USFS Rocky Mountain Research Station
#Andrew d0t Bailey a+ nps daht "common suffix for US National Park Service email addresses"
#
#Purpose: 
# This is a script that will download data exposed through an ESRI rest service and save it as a geodatabase feature class
# If the REST service has a limit of n features, the script will download n-1 features at a time and then merge the results
# This script uses ArcPy and requires ArcGIS 10.2 or higher
#
#Development Log:
#2014-12-02 Began development/comment psuedocode
#2014-12-08 Script functions, but parameters are in the script.
#2014-12-24 Completed round one of development, this is a functional script and the major elements are parameterized at the command line
#			path joins are done using os.path.join instead of + '//' +
#2014-12-30 Two bugs fixed, one enhancement added. First bug is, some services do not specify maxRecords. In that case, need to make 
#			assumption that 1000 is max and then we'll request 500 records per call. Second, it needs to read name of OID field from 
#			GetIds call and use that instead of defaulting to ObjectIDEnhancement: script should specify usage is you call it with no args.
#2015-01-20 Added error catching and reporting for failed arcpy Append, failed arcpy JSON to Features conversion,
#			and failed GetAddressInfo request. Right now, these simply print to the console, they should probably
#			log themselves using the python error logging routines. Now creating directory using random name for 
#			storage of JSON files. Fixed a platform dependency in temp file cleanup.

#Inputs:
#gdbPath - geodatabase to store output featureclass
#gdbFCName - featureclass which will be created in the GDB
#REST_URL - REST Endpoint by which data are available
#FieldNames - List of space separated field names in dataset at REST endpoint if only a subset of fields are desired. If blank, all fields will be retrieved

#Usage:
# RESTQueryToFGDB_arcpy.py gdbPath gdbFCName REST_URL field1 field2 field3
#Examples:
# RESTQueryToFGDB_arcpy.py c:\temp\restWork\newTest.gdb TNCLands20141224 http://50.18.215.52/arcgis/rest/services/nascience/tnc_lands/MapServer/1 OBJECTID
#	this will return geometry and the object ID field
# RESTQueryToFGDB_arcpy.py c:\temp\restWork\newTest.gdb TNCLands20141224 http://50.18.215.52/arcgis/rest/services/nascience/tnc_lands/MapServer/1
#	this will return geometry and all fields

#TO DO:
#There is limited error checking in this script. Add some more, at least at major fail points (FC already exists, etc)
#Catch time-outs or "bad status line" errors from invalid HTTP responses. Perhaps a 3-attempt retry.


import urllib2
import urllib
import urlparse
import httplib
import socket
import json
import pprint
import string
import random
import os
import sys
import arcpy
from subprocess import call
import traceback

#------------------------------------------------------------------------------
# SUPPORTING FUNCTIONS START HERE
# main() does not use getRecordCount(), but it is here in case needed
#------------------------------------------------------------------------------

def randomStringGenerator(size = 10, chars=string.ascii_uppercase + string.digits):
# from http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
	return "".join(random.choice(chars) for _ in range(size))
	
def getRESTServiceDescription(url):
#return service description as python dict which can be parsed
	if DEBUG: print "getting service description\n"
	
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	parameters = {'f' : 'json', }

	urlparams = urllib.urlencode(parameters)
	parts = urlparse.urlparse(url)
	h = httplib.HTTPConnection(parts.netloc)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	h.request('POST', parts.path, urlparams, headers)
	r = h.getresponse()
	return r.read()

def getAllObjectIDs(url):
# return a dict containing the field name for the object ID and a python list of objectIDs
	if DEBUG: print "Getting all object IDs\n"
	
	url = url + '/query'
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	parameters = {'where' : '1=1', 'returnIdsOnly' : 'true', 'f' : 'json'}
	urlparams = urllib.urlencode(parameters)
	parts = urlparse.urlparse(url)
	h = httplib.HTTPConnection(parts.netloc)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	h.request('POST', parts.path, urlparams, headers)
	r = h.getresponse()
	obj = json.loads(r.read())
	objectIDs = obj["objectIds"] # from http://gis.stackexchange.com/questions/112805/how-to-delete-features-records-in-arcgis-server-using-python
	oidField = obj["objectIdFieldName"]
	if DEBUG: print "Finished getting object IDs\n"
	return {'oidField':oidField, 'objectIDs':objectIDs} #not sure that a dict is the most elegant way to handle this

def getRecordCount(url):
# return a count of the total number of records
	if DEBUG: print "Getting the total number of records\n"

	url = url + '/query'
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	parameters = {'where' : '1=1', 'returnCountOnly' : 'true', 'f' : 'json'}
	urlparams = urllib.urlencode(parameters)
	parts = urlparse.urlparse(url)
	h = httplib.HTTPConnection(parts.netloc)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	h.request('POST', parts.path, urlparams, headers)
	r = h.getresponse()
	obj = json.loads(r.read())
	
	return obj["count"]
	
def query_by_objectidRange(url, fields, oidFieldName, objectIDStart=0, objectIDEnd=1001, outSR='4326'):
	""" performs a POST operation where the query is called using
        the "WHERE" method specifying a start and stop OID range.  
		If a feature service has a limit to the number of records 
		it will return, use this method to get a range of features 
		from the feature service.

        Inputs:
           :url:  - string of feature service URL
		   :fields: - the fields to be returned
		   :objectIDStart: - integer of the start whole number
           :objectIDEnd: - end range whole number
		   :outSR: - spatial reference for output geometry

        Returns:
           returns string JSON of query
		From: http://anothergisblog.blogspot.com/2013/02/query-feature-service-by-object-ids.html
    """
	url = url + '/query'
	start = int(objectIDStart)
	end = int(objectIDEnd)

	if DEBUG: print "downloading objectID " + str(objectIDStart) + " to objectID " + str(objectIDEnd) + "\n"
	
	queryString = oidFieldName + " >= " + str(objectIDStart) + " AND " + oidFieldName  + " <= " + str(objectIDEnd)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	#parameters here
	parameters = {'where' : queryString, 'f' : 'json', 'outSR' : outSR, 'outFields' : fields}

	urlparams = urllib.urlencode(parameters)
	parts = urlparse.urlparse(url)
	h = httplib.HTTPConnection(parts.netloc)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	try: # trap for failed response
		h.request('POST', parts.path, urlparams, headers)
		r = h.getresponse()
		result = r.read()
	except socket.gaierror: # could fail due to Get Address Info error
		print "Failed GetAddressInfo request, no data for OIDs " + str(objectIDStart) + " through " + str(objectIDEnd)
		result = ''	
	return result
	
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
	from http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

#------------------------------------------------------------------------------
# MAIN() STARTS HERE
#------------------------------------------------------------------------------
		
#turn on debugging output
DEBUG = False

#give user expected usage if no args specified
try:
	arg1 = sys.argv[1]
except IndexError:
	print "Usage: RESTQueryToFGDB_arcpy.py gdbPath gdbFCName REST_URL field1 field2 field3"
	print "Where:"
	print "gdbPath - geodatabase to store output featureclass"
	print "gdbFCName - featureclass which will be created in the GDB"
	print "REST_URL - REST Endpoint by which data are available"
	print "field1 field2 field3 - List of space separated field names in dataset at REST endpoint if only a subset of fields are desired. If blank, all fields will be retrieved"
	sys.exit(1)

#get location and name for output gdb and FC.
#where will we store the temp files, and what will they be named (will add start and end OID names for each file)
chunkGDBPath = os.path.dirname(sys.argv[1])
chunkGDBName = os.path.basename(sys.argv[1])
if DEBUG: print "chunkGDBPath is " + chunkGDBPath + ", and chunkGDBName is " + chunkGDBName

chunkGDBFCBase = 'tempChunk' #the base name of the feature class for temp featureclasses
#where will we store the merged gdb fc and what will it be named
#this should be an argument, too!
mergeGDBFCName = sys.argv[2]

#fail if no URL was specified
#should probably spit back the expected usage here if improperly used
try:
	if sys.argv[3] != "":
		url = sys.argv[3]
except IndexError: 
	raise Exception, "you must include an argument for the REST endpoint URL"

#what fields do we want returned?
#if the command line specified fields, put them in a comma-separated string for use in the fields argument
#if no fields are listed, get all the fields
try:
	if sys.argv[4] != "": 
		fields = ', '.join(sys.argv[4:])
except:
	if DEBUG: print "No fields were specified, therefore all fields will be requested.\n"
	fields = '*'

#store temp ESRI JSON response files in a subfolder of the same folder as the GDB,
# give the temp folder a random name, and name the JSON response files using the 
# responseBaseName with start and end OID names appended onto each file
tempFolder = randomStringGenerator()
responseFilePath = os.path.join(os.path.dirname(sys.argv[1]),tempFolder)
if not os.path.exists(responseFilePath): #could be subject to a race condition, see http://stackoverflow.com/questions/273192/check-if-a-directory-exists-and-create-it-if-necessary
	os.makedirs(responseFilePath)
	if DEBUG: print "responseFilePath is " + responseFilePath
else: print "responseFilePath " + responseFilePath + " already exists, will use existing folder"
responseFileBase = "chunkEsriJSON"
		
if __name__ == "__main__":
	if DEBUG: print "DEBUG = True; leaving temp files. Remember to clean up your mess!\n"
	#get the capabilities of the service
	serviceMetadata = json.loads(getRESTServiceDescription(url))
	
	#how much in a chunk? MaxRecordCount - 1
	try: 
		chunkLength = serviceMetadata['maxRecordCount'] / 2
		if DEBUG: print "maxRecordCount = " + str(serviceMetadata['maxRecordCount']) + " so chunk size will be " + str(chunkLength)
	except KeyError: 
		chunkLength = 500 #just use 500 if there is no maxRecordCount specified
		if DEBUG: print "maxRecordCount not specified, defaulting to 500 records per chunk"
	
	#get a list of objectIDs from the service
	oidOutput = getAllObjectIDs(url)
	oidFieldName = oidOutput['oidField']
	objectIDs = oidOutput['objectIDs']
	#sort so that we get all the OIDs in order since we're going to query for them by first and last OID in the WHERE statement	
	objectIDs.sort() 
	if DEBUG: print "Sorted objectIDs.\n"

	#add object IDs to a list of lists, each as long as the chunk of records that will be requested
	#to minimize the number of calls, you can set chunkLength to the maximum number of records that
	#the service can return
	oidChunks = chunks(objectIDs, chunkLength)

	#iterate over the list of chunks and get the data for each chunk of object IDs. 
	#create a temp featureclass for each chunk
	#merge the chunk featureclasses and clean up the temp files
	for e in list(oidChunks):
		response = query_by_objectidRange(url, fields, oidFieldName, e[0], e[len(e) - 1])
		if response == '': #Don't write a file if the service call failed and null string is returned
			print "Call to service failed. Null string returned. Skipping OIDs " + e[0] + " through " + e[len(e) - 1] + "."
		else:
			responseFileName = responseFileBase + str(e[0]) + '_' + str(e[len(e) - 1]) + '.json'
			responseFile = open(os.path.join(responseFilePath,responseFileName), "w")
			responseFile.write(response)
			responseFile.close()
			
			if DEBUG: print 'OIDs ' + str(e[0]) + ' through ' + str(e[len(e)-1])
			
			#create FGDB for output
			if not arcpy.Exists(os.path.join(chunkGDBPath, chunkGDBName)):
				arcpy.CreateFileGDB_management(chunkGDBPath, chunkGDBName, "10.0")
				if DEBUG: print "Created ESRI GDB " + os.path.join(chunkGDBPath, chunkGDBName)
		
			#convert chunk of JSON into shapefile using arcpy JSON conversion tool
			chunkGDBFCName = chunkGDBFCBase + str(e[0]) + '_' + str(e[len(e) - 1])
			try: #catch if JSON conversion fails.  Should probably try the download at least once more if 
				arcpy.JSONToFeatures_conversion(os.path.join(responseFilePath, responseFileName),os.path.join(chunkGDBPath, chunkGDBName, chunkGDBFCName))
			except arcpy.ExecuteError:
				print "Parsing JSON failed for chunk with OIDs " + str(i) + " to " + str(i + chunkLength - 1) + " : " + traceback.format_exc()
		
			#create an output file and append each chunk shapefile to it
			#if the output file doesn't exist, create it using the first chunk
			if not arcpy.Exists(os.path.join(chunkGDBPath, chunkGDBName, mergeGDBFCName)):
				arcpy.Copy_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName), os.path.join(chunkGDBPath,chunkGDBName,mergeGDBFCName))			
			else: 
				try:
					arcpy.Append_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName), os.path.join(chunkGDBPath,chunkGDBName,mergeGDBFCName), "TEST")
				except arcpy.ExecuteError:
					print "Append fail, chunk with OIDs " + str(e[0]) + " to " + str(e[len(e)-1]) + " : " + traceback.print_exc()
			if not DEBUG: #delete the intermediate outputs
				arcpy.Delete_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName))
				os.remove(os.path.join(responseFilePath, responseFileName))
	if DEBUG: print "DEBUG = True; leaving temp files. Remember to clean up your mess!\n"
	else: os.rmdir(responseFilePath)
	print "Program finished."