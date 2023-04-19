# -*- coding: utf-8 -*-
"""
@author: a.lyzin
"""

import pandas as pd
import re

path = r'' # add filepath

df = pd.read_excel(path)

df['size_1'] = ""
df['size_2'] = ""

lst1 = []
lst2 = []

for index, row in df.iterrows(): 
    cel = row['model_name']
    size1 = str(re.findall(r'R\d\dC\s\d\d\d/\d\d', cel))
    size2 = str(re.findall(r'R\d\d\s\d\d\d/\d\d', cel))
    lst1.append(size1)
    lst2.append(size2)


    
df['size_1'] = lst1
df['size_2'] = lst2


df.to_excel(r'') # add savedir & filname + xlsx