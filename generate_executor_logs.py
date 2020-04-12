#!/usr/bin/python2

import os
import sys
import subprocess
from os import path


# Useful yarn commands that we use to get the logs
#yarn application -list -appStates ALL
#yarn logs -applicationId appid

# separator between different logs from different applications.
SEPARATOR = "\n"

# get all the application ids we are interested in from yarn.
def get_application_ids():
    # Get the logs from Yarn and skip the headers.
    with open(os.devnull, "w") as devnull:
        raw_logs = subprocess.check_output("yarn application -list -appStates ALL".split(), stderr=devnull).split("\n")[2:]
        filtered = [x for x in raw_logs if "App" in x]
        applicationIds = [x.split()[0].rstrip() for x in filtered]
        return applicationIds

# generates the application logs, from yarn, and writes the file into the provided folder.
def generate_application_logs(appName, destination):
    if not path.exists(destination):
        print "Error, the provided destination is invalid: "+destination
        sys.exit(1)
    destFile = destination + os.path.sep + appName + ".log"
    with open(os.devnull, "w") as devnull:
        logs = subprocess.check_output(("yarn logs -applicationId "+appName+" -log_files stderr").split(), stderr=devnull)
        with open(destFile, "w+") as f:
            f.write(logs)

def aggregate_logs(inpath, outFile):
    with open(outFile, "w+") as out:
        for entry in os.listdir(inpath):
            with open(inpath+os.path.sep+entry, "r") as inp:
                out.write(inp.read())
                out.write(SEPARATOR)




# Main entry point.

if len(sys.argv) != 3:
    print("Usage: ./generate_executor_logs.py <separate log files dir> <name concatenated>")
    sys.exit(1)

logDir = sys.argv[1]
outfile = sys.argv[2]

appIds = get_application_ids()
current = 1
total = len(appIds)
for a in appIds:
    print "generating "+str(current)+"/"+str(total)
    generate_application_logs(a, logDir)
    current += 1

print "Aggregating the logs"
aggregate_logs(logDir, outfile)







    

