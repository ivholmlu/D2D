import requests
from pprint import pprint
from authentication import get_token
from No_sync.credentials import config

def get_week_summary(token, year, week):
  url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
  headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',
  }

  response = requests.get(url, headers=headers)
  response.raise_for_status()
  return response.json()


if __name__== "__main__":
  token = get_token()
  weeksummary= get_week_summary(token,'2017','45')
  pprint(weeksummary)
