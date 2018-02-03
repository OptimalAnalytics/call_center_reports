# %% Import Base Packages
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys,os
sns.set()
# end%%

# %%  Read in the raw data
folder = 'Sample_Reports'
rpc_fn = 'ALL_RPC-2-1-2018.xlsx'

rpc = pd.read_excel(os.path.join(folder,rpc_fn),skiprows=0,converters={'Acct Id Acc':str})
rpc.head()
# end%%

# %% Create Identification functions for apply
def ib_ob(action_type):
     if action_type.upper().startswith('T'):
         return 'OB'
     else:
         return 'IB'

rpc['IB_OB'] = rpc['Call Action Type Qcc'].apply(ib_ob)

rpc['stripped'] = rpc['Call Result Type Qcc'].apply(lambda x: x[:2])
rpc['GC'] = rpc['Created By Qcc'].apply(lambda x: x[:2])

rpc.head()
# end%%

# %% Read sheets info
def read_bucket_sheet(sheet_name, excel_file,global_names = ['Acct_Num','Delinq','Date']):
    if sheet_name in ['60 Day',
                     'Mid GC',
                     'Mid In',
                     'EPD 31+',
                     'FPD 2-30',
                     'Can 2-30',
                     'Can 31+']:
         skiprows=0
         cols = ['Acct Number','Days Delinquent','Current Date']
    elif sheet_name == 'GC-P30':
         skiprows=None
         cols = ['Acct Id Acc','Days Dlq Acf','Current Date']
    elif sheet_name == 'GC-EPD':
         skiprows=0
         cols = ['Acct Id Acc','Days Dlq Acf','Current Date']
    else:
        raise ValueError('Didnt know that sheetname')


    df = excel_file.parse(
        sheet_name=sheet_name,skiprows=skiprows,
        converters={cols[0]:str,cols[-1]:pd.to_datetime}
        )[cols].rename(columns=dict(zip(cols,global_names)))
    df['Bucket'] = sheet_name
    return df

# end%%

# %% Lets do this a little different...lets get all the bucket infor we think we may need
bucket_fn = '2_1_18Bucket.xls'
excel_bucket = pd.ExcelFile(os.path.join(folder,bucket_fn))

bucket_dfs = []
for sheet in excel_bucket.sheet_names:
    bucket_dfs.append(read_bucket_sheet(sheet,excel_bucket))

buckets = pd.concat(bucket_dfs,ignore_index=True)

buckets.dropna(subset=['Acct_Num'],inplace=True)

if any(buckets.duplicated(subset=['Acct_Num'])):
    print('You had a duplicate account number...that doesnt make a lot of sense')
    print(buckets.loc[buckets.duplicated(subset=['Acct_Num'])])
else:
    print('NO DUPLICATES IN BUCKETS...WOOT WOOT')

# end%%

# %%
rpc['Acct Id Acc']
rpc.head()

buckets.head()

all_df = pd.merge(buckets,rpc[['Acct Id Acc','IB_OB','stripped','GC']],
    how='outer',left_on='Acct_Num',right_on='Acct Id Acc')
# end%%

# %%
all_df.loc[all_df.duplicated(subset=['Acct_Num'],keep=False)]
all_df.loc[all_df['Bucket'] == 'GC-P30']

all_df.loc[all_df['Acct_Num']=='20100816539453']
# end%%

# %%
# buckets.groupby(['Bucket','Date']).count()['Acct_Num']
all_df.rename(columns={'Acct_Num':'Queue','Acct Id Acc':'RPC'},inplace=True)

all_df.groupby(['Bucket','Date'])[['Queue','RPC']].agg({'Queue':pd.Series.nunique,'RPC':[pd.Series.nunique,'count']})
all_df.groupby(['Bucket','Date']).apply(lambda x: pd.Series(dict(
    Queue_total=x['Queue'].nunique(),
    Total_RPC=x['RPC'].count(),
    Unique_RPC=x['RPC'].nunique(),
    Total_PTP=x['RPC'].loc[x['stripped']=='PP'].count(),
    Unique_PTP=x['RPC'].loc[x['stripped']=='PP'].nunique(),
    T_Outbound_RPC=x['RPC'].loc[x['IB_OB']=='OB'].count(),
    U_Outbound_RPC=x['RPC'].loc[x['IB_OB']=='OB'].nunique(),
    T_Outbound_PTP=x['RPC'].loc[(x['IB_OB']=='OB') & (x['stripped']=='PP')].count(),
    U_Outbound_PTP=x['RPC'].loc[(x['IB_OB']=='OB') & (x['stripped']=='PP')].nunique()
    )))






# all_df.groupby(['Bucket','Acct_Num','IB_OB','stripped','GC']).count()['Acct Id Acc']

# end%%
