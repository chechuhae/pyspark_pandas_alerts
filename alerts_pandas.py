#!/usr/bin/env python
# coding: utf-8

# In[1]:

# import modules
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime


# In[3]:


# read data
data = pd.read_csv('data.csv')


# In[4]:


# check data shape
data.shape


# In[10]:


# build class for checking alerts
class Alerts(object):
    
    def __init__(self, dict_of_alerts={}):
        
        self.dict_of_alerts = dict_of_alerts
    
    # function for add new alert scripts
    def add(self, name, condition, time, for_groups):
        #check type of name of script
        if type(name) != str:
            raise ValueError('Name should be type str, not type {0}'.format(type(name)))
        # check type of condition
        if type(condition) !=  int:
            raise ValueError('Condition should be type int, not type {0}'.format(type(condition)))
        # check type of period    
        list_for_period = ['h', 'min', 's', 'd', 'm', 'y']
        if time not in list_for_period:
                raise ValueError('Period should be choosen from list of values {}'.format(list_for_period))
        # check groups for groupby        
        if type(for_groups) != list:
            raise ValueError('Groups should be type list, not type {0}'.format(type(for_groups)))
            
        self.dict_of_alerts[name] = [condition, time, for_groups]
        return self
    
    def info_alert(self, data):
        # change columns of our data
        data.columns = ['error_code', 'error_message', 'severity', 
             'log_location', 'mode', 'model', 
             'graphics', 'session_id', 'sdkv', 
             'test_mode', 'flow_id', 'flow_type', 
             'sdk_date', 'publisher_id', 'game_id', 
             'bundle_id', 'appv', 'language', 
             'os', 'adv_id', 'gdpr', 
             'ccpa', 'country_code', 'date']
        # filter errors
        data = data[data['severity'] == 'Error'] # data[data['error_message'].str.contains('error')]
        # convert name of errors to numbers
        error_codes = dict(zip(data.error_message.unique(), range(data.error_message.unique().size)))
        codes_error = dict(zip(range(data.error_message.unique().size), data.error_message.unique()))
        
        data['error_code'] = data['error_message'].map(error_codes)
        # get dict of alerts for printing them
        alerts = self.dict_of_alerts
        # convert column date to datetime format
        data['date'] = data['date'].apply(lambda x: datetime.datetime.utcfromtimestamp(float(x)))
        
        for alert in alerts:
            # print name of alert
            print("-------------------", alert, "-------------------")
            # add new column for groupby
            data[alert] = pd.to_datetime(data['date']).dt.to_period(alerts[alert][1])
            # print alert for each rule
            for el in data['error_code'].unique():
                # firstly group our data for recognision number of errors
                alerts[alert][2] = [alert if el == 'date_time' else el for el in alerts[alert][2]]
    
                sr_for_alert = data[data['error_code'] == el].groupby(alerts[alert][2]).size()
                sr_for_alert_filtred = sr_for_alert[sr_for_alert >= alerts[alert][0]]
                # for each error which are over than condition send(print) alert type and index of groupby
                for num, el_1 in enumerate(sr_for_alert):
                    print('Alert: {0}, error: {1}'.format(alert, codes_error[el]), sr_for_alert.index[num])
        
        


# In[11]:


# create and add rules for alerts
new = Alerts()
new.add('alert_1', 10, 'min', ['date_time'])
new.add('alert_2', 10, 'h', ['bundle_id', 'date_time'])


# In[12]:


# run script for checking
new.info_alert(data)


# In[13]:





