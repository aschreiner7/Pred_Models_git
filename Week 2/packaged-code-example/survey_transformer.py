 # -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 08:53:28 2020

@author: nbozarth
"""
import os
import pandas as pd
from awp_modules import Loader

def main():
    
    os.chdir(".\data\\")
    
    #awp paths
    awpApre2021Q3 = "awpA_pre2021Q3.csv"
    awpApost2021Q3 = "[url]"
    awpB = "[url]"
    awp24 = "[url]"
    awp241 = "[url]"
    awp242 = "[url]"
    awp243 = "[url]"
    
    
    
    f = Loader()
    
    data = f.load(loadtype='ALCHEMER_STATIC', path = awpApre2021Q3, instance_name="A")
    dataG = f.load(loadtype='ALCHEMER_URL', path = awpApost2021Q3, instance_name="A", record_address="AWPA-post2021Q3-count.txt")
    dataB = f.load(loadtype='ALCHEMER_URL', path =awpB, instance_name="B", record_address="AWPBcount.txt")
    dataD = f.load(loadtype='THRIVELIKE_URL', path =awp24, instance_name="2.4-rc", record_address="AWP2-4count.txt")
    dataE = f.load(loadtype='THRIVELIKE_URL', path =awp241, instance_name="2.41-rc", record_address="AWP2-41count.txt")
    dataF = f.load(loadtype='THRIVELIKE_URL', path =awp242, instance_name="2.42-rc", record_address="AWP2-42count.txt")
    dataH = f.load(loadtype='THRIVELIKE_URL', path = awp243, instance_name="2.43-rc", record_address="AWP2.43count.txt")
    
    
    alchemer_dfs = [data, dataG, dataB, dataD, dataE, dataF, dataH]
    selected_columns = ['Response ID', 'Time Started', 'q5', 'q6', 'q7','Date Submitted', 'Status','Source Capture', 'field', 'version']
    
    
    for df in alchemer_dfs:
        df.drop([col for col in df.columns if col not in selected_columns], axis=1, inplace=True)
        df['date']  = pd.to_datetime(df['Date Submitted']).dt.date
    
    alchemerData = pd.concat(alchemer_dfs, ignore_index=True)
    
    alchemerData = f.stripTestCases(alchemerData)
    
    
      
    
    #export as xlsx, keep same name? 
    alchemerData.to_csv("..//transformed_data.csv")

if __name__ == '__main__':
    main()