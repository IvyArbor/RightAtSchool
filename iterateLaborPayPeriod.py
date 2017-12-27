import os
fileList = os.listdir("payPeriodReports/")
# file = fileList[0]

for file in fileList:
    print("File to Process: {}".format(file))
    os.system("python job.py LOAD_DW_FactLabor_PayPeriod")
    os.remove("payPeriodReports/" + file)