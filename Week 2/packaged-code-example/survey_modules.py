# -*- coding: utf-8 -*-
"""
Created on Thu Sep 30 10:12:53 2021

@author: nbozarth
"""

import pandas as pd
import urllib.request
import time
from datetime import datetime, timedelta, date
import os




class Loader:
    def __init__(self):
        return None
    

    def load(self, loadtype, path, instance_name, record_address=None):
        if loadtype == 'ALCHEMER_URL':
            df = self.loadAlchemerData(path, record_address, instance_name)
        elif loadtype == 'ALCHEMER_STATIC':
            df = self.loadAlchemerStatic(path, instance_name)
        elif loadtype == 'THRIVELIKE_URL':
            thrivelike_df = self.loadAlchemerData(path, record_address, instance_name)
            df = self.transformThriveLikeAWP(thrivelike_df) 
        return df
    
    def loadAlchemerData(self, sgTarget, recordPath, name):
        if os.path.exists(recordPath): 
            with open(recordPath, 'r+') as f: #r? a+?
                record = int(f.read())
        else:
            with open(recordPath, 'w+') as f:
                f.write("1")
                record = 1
        count = 0
        i=0
        while count < record:
            try:                     
                i+=1
                if i > 1:
                    time.sleep(30)
            #this export is titled "automationData" in SG
                with urllib.request.urlopen(sgTarget) as response:
                    data = pd.read_csv(response, low_memory=False, sep=',', error_bad_lines=False, skip_blank_lines=True)
                count = data[data.agility1.notnull()].shape[0]
            except:
                pass
        with open(recordPath, 'w') as f:
            f.write(str(count))
        data['version'] = name
        print(name, " data loaded.")
        return data
    
    def loadAlchemerStatic(self, sgTarget, name):
        data = pd.read_csv(sgTarget, low_memory=False, sep=',', error_bad_lines=False, skip_blank_lines=True)
        data['version'] = name
        print(name, " data loaded.")
        return data
    
    def stripTestCases(self, data):    
        #remove responses with first name or last name "test"
        data = data[data['q5'].str.lower() != "test"]
        data = data[data['q6'].str.lower() != "test"]
                    
        #remove debruce domains
        data['q7'] = data['q7'].str.lower()
        split = data['q7'].str.split("@", n = 1, expand = True)
        data["domain"] = split[1]
        data = data[data["domain"] != "mydomain.org"]
        print("Test cases removed.")
        return data
    


    
    def transformThriveLikeAWP(self, data):
        data = data.rename(columns={"Email:": "q7"})
        data['q5'] = ""
        data['q6'] = ""
        print("Thrive data transformed")
        return data
    

    


    
    