from pprint import pprint
import requests

session = requests.Session()
response = requests.post(url="http://localhost:3000/api/v3/Users/login", data={"username": "ingestor", "password":})
pprint(response.json())
token = response.json()['id']

payload = {
 "creationLocation": "/PSI/SLS/TOMCAT3",
 "creationTime": "	2021-02-10T23:56:37Z",
 "sourceFolder": "/scratch/devops",
 "type": "raw",
 "owner": "ingestor",
 "ownerGroup":"p16623",
 "contactEmail": "dmcreynolds@lbl.gov",
 "shareGroup": "foo"
}

response = session.post(url=f"http://localhost:3000/api/v3/Datasets?access_token={token}", data=payload)
pprint(response.json())
