#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from functools import reduce
from pyspark.sql import functions as F


# In[2]:


spark = pyspark.sql.SparkSession.builder.appName("SparkByExamples").getOrCreate()
data = spark.read.csv("data.csv", header=True)

# In[3]:


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
        list_for_period = ['hour', 'minute', 'second', 'day', 'month', 'year']
        if time not in list_for_period:
                raise ValueError('Period should be choosen from list of values {}'.format(list_for_period))
        # check groups for groupby        
        if type(for_groups) != list:
            raise ValueError('Groups should be type list, not type {0}'.format(type(for_groups)))
            
        self.dict_of_alerts[name] = [condition, time, for_groups]
        return self
    
    def info_alert(self, data):
        # change columns of our data
        new = ['error_code', 'error_message', 'severity', 
             'log_location', 'mode', 'model', 
             'graphics', 'session_id', 'sdkv', 
             'test_mode', 'flow_id', 'flow_type', 
             'sdk_date', 'publisher_id', 'game_id', 
             'bundle_id', 'appv', 'language', 
             'os', 'adv_id', 'gdpr', 
             'ccpa', 'country_code', 'date']
        data = data.toDF(*new)
        # filter Errors
        data = data.filter(F.col('severity') == 'Error')
        # convert name of errors to numbers
        messages = data.select(F.collect_set('error_message')).first()[0]
        number_of_messages = len(messages)
        error_codes = dict(zip(messages, [str(el) for el in range(number_of_messages)]))
        codes_error = dict(zip([str(el) for el in range(number_of_messages)], messages))
        
        data = data.withColumn('error_code', F.col('error_message'))
        data = data.replace(error_codes, 1, 'error_code')
        # get dict of alerts for printing them
        alerts = self.dict_of_alerts
        # convert column date to datetime format
        data = data.withColumn('date', F.from_unixtime(F.col('date')))
        
        for alert in alerts:
            # print name of alert
            print("-------------------", alert, "-------------------")
            # add new column for groupby
            data = data.withColumn(alert, F.date_trunc(alerts[alert][1], data.date))
            
            # print alert for each rule
            for el in data.select(F.collect_set('error_code')).first()[0]:
                # firstly group our data for recognision number of errors
                alerts[alert][2] = [alert if el == 'date_time' else el for el in alerts[alert][2]]
                                                                       
                
                sr_for_alert = data.filter(F.col('error_code') == el).groupBy(alerts[alert][2]).count()
                sr_for_alert_filtred = sr_for_alert.filter(F.col('count') >= alerts[alert][0]).rdd.collect()
                # for each error which are over than condition send(print) alert type and index of groupby
                for el_1 in sr_for_alert_filtred:
                    print('Alert: {0}, error: {1}'.format(alert, codes_error[el]), el_1[0:-1])
        
        


# In[14]:


# create and add rules for alerts
new = Alerts()
# date_time is generic type for our datetime period
new.add('alert_1', 10, 'minute', ['date_time']) # here date_time is minure
new.add('alert_2', 10, 'hour', ['bundle_id', 'date_time']) # here date_time is hour


# In[15]:


# run script for checking
new.info_alert(data)


# In[ ]:




