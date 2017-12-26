import os

fileList = os.listdir("laborReports/")
# file = fileList[0]

for file in fileList:
    print("File to Process: {}".format(file))
    os.system("python job.py LOAD_DW_FactLabor")
    os.remove("laborReports/" + file)