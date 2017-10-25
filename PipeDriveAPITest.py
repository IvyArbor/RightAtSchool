import requests
import json

#getting data from API and saving them as JSON files on folder sources


#call to deals api
response = requests.get('https://companydomain.pipedrive.com/v1/deals/?api_token=5119919dca43c62ca026750611806c707f78a745', auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'),
                        headers = {'Content-Type': 'application/json'})

print("DEALS: " + response.text)

#save as JSON file
with open('sources/Pipedrive_Deals.json', 'w') as outfile:
   json.dump(response.json(), outfile)

#--------------------------------------------------------------------------------------------------------------

#call to organizations api
response = requests.get('https://companydomain.pipedrive.com/v1/organizations/?api_token=5119919dca43c62ca026750611806c707f78a745', auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'),
                        headers = {'Content-Type': 'application/json'})

print("ORGANIZATIONS: " + response.text)

#save as JSON file
with open('sources/Pipedrive_Organizations.json', 'w') as outfile:
   json.dump(response.json(), outfile)

#---------------------------------------------------------------------------------------------------------------------------

#call to persons api
response = requests.get('https://companydomain.pipedrive.com/v1/persons/?api_token=5119919dca43c62ca026750611806c707f78a745', auth=('Right At School', '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'),
                        headers = {'Content-Type': 'application/json'})

print("PERSONS: " + response.text)

#save as JSON file
with open('sources/Pipedrive_Persons.json', 'w') as outfile:
   json.dump(response.json(), outfile)