import numpy as np
import pandas as pd
import nltk
import re


from nltk.corpus import stopwords


df=pd.read_csv("swm/data1.csv")

df=df.drop(["tweet_type","language"],axis=1)
df.dropna()
text=df.tweet_text
pattern1 = r'@\w+\b'
pattern2 = r'http\S+'
pattern3 = r'[^a-zA-Z\s]'
pattern4 = re.compile("["
                         u"\U0001F600-\U0001F64F"  # Emojis
                         u"\U0001F300-\U0001F5FF"  # Symbols & Pictographs
                         u"\U0001F680-\U0001F6FF"  # Transport & Map Symbols
                         u"\U0001F1E0-\U0001F1FF"  # Flags (iOS)
                         u"\U00002500-\U00002BEF"  # Chinese/Japanese/Korean characters
                         u"\U00002702-\U000027B0"
                         u"\U00002702-\U000027B0"
                         u"\U000024C2-\U0001F251"
                         u"\U0001f926-\U0001f937"
                         u"\U00010000-\U0010ffff"
                         u"\u2640-\u2642"
                         u"\u2600-\u2B55"
                         u"\u200d"
                         u"\u23cf"
                         u"\u23e9"
                         u"\u231a"
                         u"\ufe0f"  # dingbats
                         u"\u3030"
                         "]+", flags=re.UNICODE)

for i in range(len(text)):
    text[i]=str(text[i])
    text[i]=re.sub(pattern1, '', text[i])
    text[i]=re.sub(pattern2, '', text[i])
    text[i]=re.sub(pattern3,'',text[i])
    text[i]=pattern4.sub(r'', text[i])
    text[i]=text[i].lower()
    print(text[i])

df['text']=text
df=df.drop(['tweet_text'],axis=1)

df.to_csv("C:\swm\clean.csv")





