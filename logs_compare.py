import os
import argparse
import sys
import pandas as pd
import numpy as np
import re

def getLogs(folderDir):
	returnDF = pd.DataFrame(columns = ['name', 'record', 'duration', 'error'])

	for filename in os.listdir(folderDir):
		logFile = os.path.join(folderDir, filename)
		if logFile.endswith('.log'):	
			rowRecord = ""
			rowDuration = ""
			rowError = "" 
			with open(logFile) as file:
				for line in file:
					if 'rows copied' in line.lower():
						rowRecord = re.search(r"^(\d+) rows copied\.",line).group(1)
					elif 'clock time' in line.lower():
						rowDuration = re.search(r"Clock.*?[tT]otal.*?(\d+)",line).group(1)
					elif 'starting copy' not in line.lower() and 'rows sent to sql server' not in line.lower() and 'rows successfully' not in line.lower() and 'network packet size' not in line.lower()and line.strip() != "":
						if line not in rowError:
							rowError = rowError+line

			df = pd.DataFrame({'name': [filename.replace('.log','')], 'record': [rowRecord], 'duration': [rowDuration], 'error':[rowError]})
			returnDF = pd.concat([returnDF,df],ignore_index=True)
	return returnDF


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	checkArgs = True

	# assign argument
	parser.add_argument('-e','--exportDir', type=str, required=True)
	parser.add_argument('-i','--importDir', type=str, required=True)
	parser.add_argument('-o','--outputDir', type=str, default=".")
	parser.add_argument('-n','--outputName', type=str, default="compare_logs.csv")
	flags = parser.parse_args()

	if os.path.isdir(flags.exportDir) == False:
		print(f"Export's directory {flags.exportDir} does not exist")
		checkArgs = False

	if os.path.isdir(flags.importDir) == False:
		print(f"Import's directory {flags.importDir} does not exist")
		checkArgs = False

	if checkArgs == False:
		sys.exit()  

	exportDF = getLogs(flags.exportDir)
	importDF = getLogs(flags.importDir)

	finalDF = pd.merge(exportDF, importDF, on=['name'], how='outer', suffixes=['_export', '_import'])
	finalDF['match'] = np.where(finalDF['record_export'] == finalDF['record_import'],'true','false')
	finalDF = finalDF.sort_values('name')

	finalDF.to_csv(f"{flags.outputDir}/{flags.outputName}".replace('//','/'), index=False)
