import requests
import json

#getting data from API and saving them as JSON files on folder sources

#call to record api
response = requests.get('https://collabornation.net/lms/api/0.06/record.json',auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'))

print("RECORD: " + response.text)

#save as JSON file
with open('sources/CyperWorx_Record.json', 'w') as outfile:
   json.dump(response.json(), outfile)

#-------------------------------------------------------------------------------------------
#call to user api
response = requests.get('https://collabornation.net/lms/api/0.06/user.json',auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'))

print("USER: " + response.text)

#save as JSON file
with open('sources/CyperWorx_User.json', 'w') as outfile:
   json.dump(response.json(), outfile)


#-------------------------------------------------------------------------------------------
#call to course api
response = requests.get('https://collabornation.net/lms/api/0.06/course.json',auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'))

print("COURSE: " + response.text)

#save as JSON file
with open('sources/CyperWorx_Course.json', 'w') as outfile:
   json.dump(response.json(), outfile)