# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 08:44:42 2023

@author: H2EDTDLW
"""
import requests




class CDAPostTS:
    def __init__(self,name,officeID,units):
        self.name = name
        self.officeID = officeID
        self.units = units
        self.values = []
    def insertValue(self,datetime,value,quality=0):
        self.values.append([datetime,value,quality])
    def post(self,API_KEY,API_URL):
        headers = {
            "accept": "*/*",
            "Content-Type": "application/json;version=2",
            "Authorization": "apikey " + API_KEY,
        }
        payload = {'name':self.name,
                   'office-id':self.officeID,
                   'units':self.units,
                   'values':self.values}
        with requests.Session() as s:
            r = s.post(
                url=API_URL + "timeseries",
                headers=headers,
                json=payload,
                verify="CDA.pem",
            )
        return (r.status_code,r.text)
