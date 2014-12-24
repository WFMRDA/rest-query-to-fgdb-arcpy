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

#Inputs:
#gdbPath - geodatabase to store output featureclass
#gdbFCName - featureclass which will be created in the GDB
#REST_URL - REST Endpoint by which data are available
#FieldNames - List of space separated field names in dataset at REST endpoint if only a subset of fields are desired. If blank, all fields will be retrieved

#Usage:
# cmdLine_RESTQueryToFGDB_arcpy.py gdbPath gdbFCName REST_URL field1 field2 field3
#Examples:
# cmdLine_RESTQueryToFGDB_arcpy.py c:\temp\restWork\newTest.gdb TNCLands20141224 http://50.18.215.52/arcgis/rest/services/nascience/tnc_lands/MapServer/1 OBJECTID
#	this will return geometry and the object ID field
# cmdLine_RESTQueryToFGDB_arcpy.py c:\temp\restWork\newTest.gdb TNCLands20141224 http://50.18.215.52/arcgis/rest/services/nascience/tnc_lands/MapServer/1
#	this will return geometry and all fields

#TO DO:
#There is very little error checking in this script. Add some, at least at the major fail points (GDB already exists, FC already exists, etc)

import urllib2
import urllib
import urlparse
import httplib
import json
import pprint
import os
import sys
import arcpy
from subprocess import call

#turn on debugging output
DEBUG = False

#get location and name for output gdb and FC.
#where will we store the temp files, and what will they be named (will add start and end OID names for each file)
chunkGDBPath = os.path.dirname(sys.argv[1])
chunkGDBName = os.path.basename(sys.argv[1])
if DEBUG: print "chunkGDBPath is " + chunkGDBPath + ", and chunkGDBName is " + chunkGDBName

chunkGDBFCBase = 'test'
#where will we store the merged gdb fc and what will it be named
#this should be an argument, too!
mergeGDBFCName = sys.argv[2]

#fail if no URL was specified
#should probably spit back the expected usage here if improperly used
try:
	if sys.argv[3] != "":
		url = sys.argv[3]
except: 
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

#where will we store the temp ESRI JSON response files, and what will we name them (will add start and end OID names for each file)
responseFilePath = os.path.dirname(sys.argv[1])
if DEBUG: print "responseFilePath is " + responseFilePath
responseFileBase = "chunkEsriJSON"


def getRESTServiceDescription(url):
#return service description as python struct which can be parsed
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
# return a python list of objectIDs
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

	return objectIDs
	
def query_by_objectidRange(url, fields, objectIDStart=0, objectIDEnd=1001, outSR='4326'):
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
	
	queryString = "OBJECTID >= " + str(objectIDStart) + " AND OBJECTID <= " + str(objectIDEnd)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	#parameters here
	parameters = {'where' : queryString, 'f' : 'json', 'outSR' : outSR, 'outFields' : fields}

	urlparams = urllib.urlencode(parameters)
	parts = urlparse.urlparse(url)
	h = httplib.HTTPConnection(parts.netloc)
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	h.request('POST', parts.path, urlparams, headers)
	r = h.getresponse()
	return r.read()
	
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
	from http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
	
if __name__ == "__main__":
	#get the capabilities of the service
	serviceMetadata = json.loads(getRESTServiceDescription(url))
	
	#how much in a chunk? MaxRecordCount - 1
	chunkLength = serviceMetadata['maxRecordCount'] / 2
	if DEBUG: print "maxRecordCount = " + str(serviceMetadata['maxRecordCount']) + " so chunk size will be " + str(chunkLength)
	
	#get a list of objectIDs from the service
	objectIDs = getAllObjectIDs(url)
	#sort so that we get all the OIDs in order since we're going to query for them by first and last OID in the WHERE statement	
	objectIDs.sort() 

	#add object IDs to a list of lists, each as long as the chunk of records that will be requested
	#to minimize the number of calls, you can set chunkLength to the maximum number of records that
	#the service can return
	oidChunks = chunks(objectIDs, chunkLength)

	#iterate over the list of chunks and get the data for each chunk of object IDs. 
	#create a temp shapefile for each chunk
	#merge the chunk shapefiles and clean up the temp files
	for e in list(oidChunks):
		response = query_by_objectidRange(url, fields, e[0], e[len(e) - 1])
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
		arcpy.JSONToFeatures_conversion(os.path.join(responseFilePath, responseFileName),os.path.join(chunkGDBPath, chunkGDBName, chunkGDBFCName))
		
		#create an output file and append each chunk shapefile to it
		#if the output file doesn't exist, create it using the first chunk
		if not arcpy.Exists(os.path.join(chunkGDBPath, chunkGDBName, mergeGDBFCName)):
			arcpy.Copy_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName), os.path.join(chunkGDBPath,chunkGDBName,mergeGDBFCName))			
		else: 
			arcpy.Append_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName), os.path.join(chunkGDBPath,chunkGDBName,mergeGDBFCName), "TEST")
			
		if not DEBUG: #delete the intermediate outputs
			arcpy.Delete_management(os.path.join(chunkGDBPath,chunkGDBName,chunkGDBFCName))
			call('del ' + os.path.join(responseFilePath, responseFileName), shell=True)
		else: print "DEBUG = True; leaving temp files. Remember to clean up your mess!\n"