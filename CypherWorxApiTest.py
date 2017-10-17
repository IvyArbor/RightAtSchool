import requests
import json

#call to api
response = requests.get('https://collabornation.net/lms/api/0.06/record.json',auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'))

print(response.text)

#records = requests.get(url ='https://collabornation.net/lms/api/0.06/record', data=response,headers={"Content-Type": "application/json"} )

with open('sources/CyperWorx_Extract.json', 'w') as outfile:
   json.dump(response.json(), outfile)