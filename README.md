rest-query-to-fgdb-arcpy
========================
This is a script that will download data exposed through an ESRI rest
service and save it as a geodatabase feature class. If the REST service
has a limit of n features, the script will download n/2 features at a
time and then merge the results. This script uses ArcPy and requires
ArcGIS 10.2 or higher.

TO DO:  There is limited error checking in this script. Add some more, at least at major fail points (FC already exists, etc). Catch time-outs or "bad status line" errors from invalid HTTP responses. Perhaps a 3-attempt retry.

Inputs:
gdbPath - geodatabase to store output featureclass
gdbFCName - featureclass which will be created in the GDB
REST_URL - REST Endpoint by which data are available
FieldNames - List of space separated field names in dataset at REST endpoint if only a subset of fields are desired. If blank, all fields will be retrieved

Usage:
RESTQueryToFGDB_arcpy.py gdbPath gdbFCName REST_URL field1 field2 field3

Example:
RESTQueryToFGDB_arcpy.py c:\importantDynamicData\mostimportantDatabase.gdb criticalDataset https://10.10.10.10/arcgis/rest/services/bigServer/dataland/MapServer/1 OBJECTID NAME STATE NOTES

