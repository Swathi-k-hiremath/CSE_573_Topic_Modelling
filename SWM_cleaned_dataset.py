#!/usr/bin/env python
# coding: utf-8

# In[83]:


import pandas as pd
import numpy as np
df=pd.read_csv("clean_swm.csv")
df.head()


# In[84]:


df=df.drop(["Unnamed: 0"],axis=1)


# In[85]:


df.head()


# In[86]:


pd.isnull(df).value_counts()


# In[87]:


df.shape


# In[88]:


df.nunique()


# In[89]:


df=df.dropna()


# In[90]:


df.shape


# In[ ]:





# In[91]:


len(text)


# In[92]:


df = df.reset_index()


# In[95]:


df=df.drop(["index"],axis=1)
df.head(10)


# In[96]:


text=df.text
val=[]


# In[97]:


text[7]


# In[98]:


type(text[0])


# In[99]:


len(text[0])
v=text[49].split(" ")
print(v)


# In[100]:


for i in range(len(text)):
    text[i]=text[i].strip()
    temp=text[i].split(" ")
    if text[i]!="nan" and len(temp)>3:
        val.append(text[i])


# In[101]:


len(val)


# In[102]:


len(val)


# In[103]:


type(val)


# In[104]:


val = list(dict.fromkeys(val))


# In[105]:


len(val)


# In[107]:


val=np.array(val)


# In[108]:


val


# In[111]:


data=pd.DataFrame(data=val)
data.head()


# In[112]:


data.to_csv("swm_clean_data.csv")


# In[ ]:




