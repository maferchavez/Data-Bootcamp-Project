import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/maferchavez/Data-Bootcamp-Project/main/raw_data/user_purchase.zip")
#print (df)
#file = df.to_csv('user_purchase.csv', index=False)#encoding='utf-8')
print (df.to_csv('user_purchase.csv', index=False))
print (type(file))
#with open(file) as f:
 #   print (type(f))
 #   next (f)
#    print (f)