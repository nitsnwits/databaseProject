# ODBC type code sample for python
# using fire dataset, i'll create a table in postgresql, try to connect to it, load the firedataset in it
# read it back and do some queries, 
# this should server as a boiler plate code for project 1

# Neeraj, SJSU

#Make a global hash with UUID as the key and an array of values as rest of elements
#Iterate over that hash to make insert statements to database

import sys
import uuid
import os
import csv
from collections import defaultdict

#Global variables

#To check if directory of files is given as first arguments, else take current dir default
sourceFilesPath = sys.argv[1] if len(sys.argv) > 1 else ".";
fileOpenMode = 'rb' #just saying, if we needed to change the mode
parsedDataSet = defaultdict(list) #this would be final hash to use to insert rows in DB

def generateUUID():
    return str(uuid.uuid4());
    
def readFile(filename):
    #Read one file and add it to memory hash
    with open(sourceFilesPath + filename, fileOpenMode) as csvfile:
        rows = csv.reader(csvfile);
        try:
            rows.next(); #ignore headers
            for row in rows:
                if row:
                    parsedDataSet[generateUUID()].extend(row);
        except StopIteration:
            print "WARN: Found empty file: " + filename

def readAllFiles():
    filesList = os.listdir(sourceFilesPath);
    for file in filesList:
        readFile(file);
    return;

readAllFiles();

i=0
for key, val in parsedDataSet.iteritems():
    print key + '->' + str(val);
    if i > 10:
        break;
    else:
        i += 1;
    

    
    