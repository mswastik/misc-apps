import dropbox
from dropbox import files
import requests
import base64
import json

APP_KEY = 'bfc7gt2wmbd081b'
APP_SECRET = 'lobt5soielr6u1y'
ACCESS_CODE_GENERATED = '_3-tzlmvZh4AAAAAAAABvqIDtvyQ2Jv5L0S0Bt8tJb4'
BASIC_AUTH = base64.b64encode(f'{APP_KEY}:{APP_SECRET}'.encode())
headers = {
    'Authorization': f"Basic {BASIC_AUTH}",
    'Content-Type': 'application/x-www-form-urlencoded',
}
data = f'code={ACCESS_CODE_GENERATED}&grant_type=authorization_code'

dd={
  "access_token": "sl.BtuGHJugT_O17vIm435pJMpaHz7AXIwJ5dwMzLiCCDPbXDJQejK2-O32_ddzBopPqgpPPq_9XsUSwkJjn2_IzjLnBDK-86sAhN-namCmITXCFY5-AwNrIEa30P4xS6tXWtvr9L8YLgSjF8Nli2xZsI8",
  "token_type": "bearer",
  "expires_in": 14400,
  "refresh_token": "o7ZbsNFsH4YAAAAAAAAAARShWlKOohLYoKGi0375MI9gcKVOafHLkRDUUzaaURt5",
  "scope": "account_info.read account_info.write contacts.read contacts.write file_requests.read file_requests.write files.content.read files.content.write files.metadata.read files.metadata.write",
  "uid": "28284714",
  "account_id": "dbid:AABK-WoaRoNWNqqX2a-5h8kCMnmSzdDolCw"
}

url=f'https://api.dropbox.com/oauth2/token?grant_type=refresh_token&refresh_token={dd["refresh_token"]}&client_id=bfc7gt2wmbd081b&client_secret=lobt5soielr6u1y'
response = requests.post(url)
tok=json.loads(response.content)['access_token']
dbx = dropbox.Dropbox(tok)
md, res = dbx.files_download('/user.json')
data=json.loads(res.content)