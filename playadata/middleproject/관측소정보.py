#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sys
{sys.prefix}


# In[3]:


get_ipython().system('conda install -c conda-forge -y --prefix {sys.prefix} pandas')


# In[4]:


import urllib.request 
import json 
import pandas as pd 
from pandas.io.json import json_normalize 


# In[5]:


service_key="1LJG14JWnAylzKi5GvmuGg=="
url="http://www.khoa.go.kr/api/oceangrid/ObsServiceObj/search.do?ServiceKey="+service_key+"&ResultType=json"
url


# In[6]:


df = pd.DataFrame()


# In[7]:


response = urllib.request.urlopen(url) 
json_str = response.read().decode("utf-8")

json_object = json.loads(json_str)


# In[14]:


json_str[0:500]


# In[11]:


result_df = pd.json_normalize(json_object['result']['data'])
result_df.head(5)


# In[15]:


result_df.to_csv("관측소 정보.csv", encoding = 'utf-8-sig') 


# In[ ]:




