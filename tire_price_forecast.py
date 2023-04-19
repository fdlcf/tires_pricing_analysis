# coding: utf-8

import sys
import psycopg2
import pandas as pd
import time
import plotly.express as px
from scipy import stats
import nbconvert
import numpy as np

# db connection params
conn_param_1 = {
    "host"      : "000.000.000.000",
    "database"  : "dwh",
    "user"      : "user",
    "password"  : ""
}

conn_param_2 = {
    "host"      : "000.0.000.000",
    "database"  : "dwh",
    "user"      : "user",
    "password"  : ""
}


sql = """
WITH tb AS (SELECT 
model.id,
model.brand,
brand.class,
model.model_name,
model.season,
model.vendor_code,
CASE WHEN model.is_run_flat IS TRUE THEN 'Y' ELSE 'N' END AS run_flat,
CASE WHEN model.is_studded IS TRUE THEN 'Y' ELSE 'N' END AS studed,
model.size_raw,
model.size,
diameter,
cost.price_net
FROM smr.tire_models_winter as model
JOIN smr.tire_brand as brand ON brand.id = model.brand
JOIN smr.tire_tirecost as cost ON cost.tire_id = model.id
WHERE brand.is_prohibited IS FALSE AND cost.date = '2023-03-10'::date)

SELECT 
id,
CONCAT(size, class, run_flat) AS cost_group,
brand,
class,
model_name,
season,
vendor_code,
size,
size_raw,
run_flat,
diameter,
price_net
FROM tb
"""


def connect_to_psql(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn


def get_normal_tyre_price(input_df): 
    classes = input_df['cost_group'].unique()
    res = pd.DataFrame()
    for i in classes:
        cost_class = i
        df = input_df.query('cost_group == @cost_class')
        avg_cost = df['price_net'].mean()
        devider = avg_cost / 10
        df['gr'] = round(df['price_net'] / devider, 0)
        df = df.astype({'gr': str, 'cost_group': str})
        group = df.groupby(['gr']).agg({'cost_group':'first', 'class':'first', 'run_flat':'first', \
                                        'season':'first', 'size':'first', 'size_raw':'first', 'price_net': 'mean', 'id': 'count'})
        qty_col = group['id']
        price_col = group['price_net']
        max_qty_finder = qty_col.max()
        max_qty = group.query('id == @max_qty_finder')
        max_qty = max_qty.groupby(['cost_group']).agg({'class':'first', 'run_flat':'first', \
                                                       'season':'first', 'size':'first', 'size_raw':'first', 'price_net': 'mean', 'id': 'sum'})
        max_qty['price_net'] = max_qty['price_net'] * 1.04 # indexing price for 4%
        #print(max_qty)
        res = res.append(max_qty)
    return res


def get_luxury_tyre_price(input_df): 
    classes = input_df['cost_group'].unique()
    res = pd.DataFrame()
    for i in classes:
        cost_class = i
        df = input_df.query('cost_group == @cost_class')
        df = df.astype({'cost_group': str})
        group = df.groupby(['cost_group']).agg({'class':'first', 'run_flat':'first', 'season':'first', \
                                                'size_raw':'first', 'size':'first', 'price_net': 'max', 'id': 'count'})
        group['price_net'] = group['price_net'] * 1.03 # indexing price for 3%
        res = res.append(group)
    return res   


def get_cost_indexed_3y(input_df):
    input_df['indexed_cost_3y'] = ""
    for index, row in input_df.iterrows():
        a = row['price_net']
        b = a * 1.1
        c = b * 1.1
        indx = (a + b + c)/3
        input_df.at[index,'indexed_cost_3y'] = indx
        
    return input_df


def get_cost_indexed_2y(input_df):
    input_df['indexed_cost_2y'] = ""
    for index, row in input_df.iterrows():
        a = row['price_net']
        b = a * 1.1
        c = b * 1.1
        indx = (a + b)/2
        input_df.at[index,'indexed_cost_2y'] = indx
        
    return input_df

#connect to db
conn_oper = connect_to_psql(conn_param_2)
# get data
df = pd.read_sql(sql, conn_oper)

#split by size
tyre_normal = df.query('diameter < 20')
tyre_luxury = df.query('diameter >= 20')

#define actual cost
regular_size_tirecost = get_normal_tyre_price(tyre_normal)
large_size_tirecost = get_luxury_tyre_price(tyre_luxury)

tires_cost = pd.concat([regular_size_tirecost, large_size_tirecost])

# indexing cost
tires_cost = get_cost_indexed_3y(tires_cost)
tires_cost = get_cost_indexed_2y(tires_cost)


# handling result table
sizes = pd.DataFrame(tires_cost['size_raw'].unique(), columns=['size_raw'])


standard = tires_cost[(tires_cost['class']=='Standard')&(tires_cost['run_flat']=='N')][['size_raw', 'indexed_cost_3y', 'indexed_cost_2y']]
standard = standard.rename(columns={'indexed_cost_3y':'standard_indexed_cost_3y', 'indexed_cost_2y': 'standard_indexed_cost_2y'})

standard_rf = tires_cost[(tires_cost['class']=='Standard')&(tires_cost['run_flat']=='Y')][['size_raw', 'indexed_cost_3y', 'indexed_cost_2y']]
standard_rf = standard_rf.rename(columns={'indexed_cost_3y':'standard_rf_indexed_cost_3y', 'indexed_cost_2y': 'standard_rf_indexed_cost_2y'})

premium = tires_cost[(tires_cost['class']=='Premium')&(tires_cost['run_flat']=='N')][['size_raw', 'indexed_cost_3y', 'indexed_cost_2y']]
premium = premium.rename(columns={'indexed_cost_3y':'premium_rf_indexed_cost_3y', 'indexed_cost_2y': 'premium_indexed_cost_2y'})

premium_rf = tires_cost[(tires_cost['class']=='Premium')&(tires_cost['run_flat']=='Y')][['size_raw', 'indexed_cost_3y', 'indexed_cost_2y']]
premium_rf = premium_rf.rename(columns={'indexed_cost_3y':'premium_rf_indexed_cost_3y', 'indexed_cost_2y': 'premium_rf_indexed_cost_2y'})


sizes = sizes.merge(standard, how='left', left_on='size_raw', right_on='size_raw')
sizes = sizes.merge(standard_rf, how='left', left_on='size_raw', right_on='size_raw')
sizes = sizes.merge(premium, how='left', left_on='size_raw', right_on='size_raw')
sizes = sizes.merge(premium_rf, how='left', left_on='size_raw', right_on='size_raw')

# save result to excel
sizes.to_excel(r'')# add savedir & filename + .xlsx

