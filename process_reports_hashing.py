# %% Import Base Packages
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys,os
from process_reports import read_bucket_sheet
from colored_logger import customLogger
logger = customLogger('root')
# end%%

# %% Get inputs
rpc_fn = os.path.join('Sample_Reports','ALL_RPC-2-1-2018.xlsx')
bucket_fn = os.path.join('Sample_Reports','2_1_18Bucket.xls')
# end%%

# %%
rpc = pd.read_excel(rpc_fn,skiprows=0,converters={'Acct Id Acc':str})
# end%%

# %% Get Bucket file
bucket_dfs = []
excel_bucket = pd.ExcelFile(bucket_fn)
for sheet in excel_bucket.sheet_names:
    bucket_dfs.append(read_bucket_sheet(sheet,excel_bucket))
buckets = pd.concat(bucket_dfs,ignore_index=True)
buckets.dropna(subset=['Acct_Num'],inplace=True)
# buckets['Acct_Num_new'] = buckets.index.map(lambda x: 'Acct {}'.format(x))
# acct_dict = buckets[['Acct_Num','Acct_Num_new']].set_index('Acct_Num')['Acct_Num_new'].to_dict()
buckets
# end%%

# %% Now we need to get all acct numbers and replace them
all_act = pd.concat([buckets['Acct_Num'],rpc['Acct Id Acc']],ignore_index=True)
all_act.dropna(inplace=True)
all_act.drop_duplicates(inplace=True)
all_act = all_act.to_frame('Acct_Num')

all_act['Acct_Num_new'] = all_act.index.map(lambda x: 'Acct {}'.format(x))

all_act.set_index('Acct_Num',inplace=True)
d = all_act['Acct_Num_new'].to_dict()
# acct_dict = all_act[['Acct_Num','Acct_Num_new']].set_index('Acct_Num').to_dict()

rpc['Acct Id Acc'] = rpc['Acct Id Acc'].apply(lambda x: d[x])
buckets['Acct_Num'] = buckets['Acct_Num'].map(lambda x: d[x])

any(buckets['Acct_Num'].isnull())
any(rpc['Acct Id Acc'].isnull())
# end%%

# %% Now we need to do the same with the agent names
# agents = rpc['Created By Qcc'].copy()
# buckets['Associate'].dropna().unique()
agents = pd.concat([buckets['Associate'],rpc['Created By Qcc']],ignore_index=True)
agents.dropna(inplace=True)
agents.drop_duplicates(inplace=True)

agents = agents.to_frame('Agents')
agents['Agents_new'] = agents.index.map(lambda x: 'Agent {}'.format(x))

agents['Agents_new'] = agents.apply(lambda x: '{}-{}'.format(x['Agents'][:2],x['Agents_new']),axis=1)
agents.set_index('Agents',inplace=True)
# d = agents['Agents_new'].to_dict()
agents
rpc['Created By Qcc'] = rpc['Created By Qcc'].map(agents['Agents_new'])
buckets['Associate'] = buckets['Associate'].map(agents['Agents_new'])

any(rpc['Created By Qcc'].isnull())
any(buckets['Associate'].isnull())

buckets['Associate'].unique()
# end%%

# %% REmove all bucket associates
# agents
#
# associates = buckets['Associate'].copy()
# associates.dropna(inplace=True)
# associates.drop_duplicates(inplace=True)


# associates = associates.to_frame('Associates')
# associates['Associates_new'] = associates.index.map(lambda x: 'Associate {}'.format(x))
# associates.set_index('Associates',inplace=True)
# temp = buckets['Associate'].map(associates['Associates_new'])
# buckets['Associate'] = temp
# end%%

# %% Write RPC to file
rpc.to_excel(os.path.join('Sample_Reports','ALL_RPC-2-1-2018_Scrubbed.xlsx'),index=False)
# end%%

buckets.to_excel(os.path.join('Sample_Reports','Buckets_Scrubbed_temp.xlsx'),index=False)
