# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 15:46:44 2024

@author: barbosad
"""

import requests

url = 'https://api.sensortower.com/v1/custom_fields_filter?auth_token=ST0_KVXVvQ3Q_9heygNnHCE2sQp'

payload = {
  "custom_fields": [
    {
      "name": "Game Genre",
      "global": True,
      "values": [
        "Shooter"
      ],
      "exclude": False
    }
  ]
}

headers = {
    'Content-Type': 'application/json'
}

response = requests.post(url, json=payload, headers=headers)

print(response.status_code)
print(response.json())