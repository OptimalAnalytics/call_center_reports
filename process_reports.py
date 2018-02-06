# %% Import Base Packages
import numpy as np
import pandas as pd
import sys,os
import argparse
import datetime
import logging
import traceback
# end%%

# %% main
def main():

    #Set up logger
    logging.basicConfig(
        level=logging.DEBUG,
        filename='temp_log.log',
        filemode='w',
        format='%(asctime)s - %(levelname)s - %(message)s')
    sys.excepthook = log_uncaught_exceptions

    logging.debug('process_reports - STARTING SCRIPT')
    # Parse inputs
    parser = argparse_logger(
        description='Read in the main files to create RPC summary')

    parser.add_argument('rpc_fn',metavar='RPC_INPUT_PATH',
        type=is_valid_file,
        help='Needs to be the full or relative path to the RPC excel file')
    parser.add_argument('bucket_fn',metavar='BUCKET_INPUT_PATH',
        type=is_valid_file,
        help='Needs to be the full or relative path to the Buckets excel file')
    parser.add_argument('-o','--output_fn',type=str,
        default='%s_RPC_summary.csv'%datetime.date.today().strftime('%Y_%m_%d'),
        help='Output file location, defaults to YYYY_MM_DD_RPC_summary.csv')

    args = parser.parse_args()

    rpc_fn = args.rpc_fn
    bucket_fn = args.bucket_fn
    output_fn = args.output_fn

    # rpc_fn = os.path.join('Sample_Reports','ALL_RPC-2-1-2018.xlsx')
    # bucket_fn = os.path.join('Sample_Reports','2_1_18Bucket.xls')
    # output_fn = os.path.join('Sample_Reports','super.csv')

    #Read in RPC file
    rpc = pd.read_excel(rpc_fn,skiprows=0,converters={'Acct Id Acc':str})
    rpc.head()
    # Process some of the RPC data to make it more useful
    def ib_ob(action_type):
        if action_type.upper().startswith('T'):
            return 'OB'
        else:
            return 'IB'

    rpc['IB_OB'] = rpc['Call Action Type Qcc'].apply(ib_ob)

    rpc['stripped'] = rpc['Call Result Type Qcc'].apply(lambda x: x[:2])
    rpc['GC'] = rpc['Created By Qcc'].apply(lambda x: x[:2])
    rpc.rename(columns={'Created By Qcc':'Agent'},inplace=True)

    #Get all the buckets
    excel_bucket = pd.ExcelFile(bucket_fn)

    bucket_dfs = []
    for sheet in excel_bucket.sheet_names:
        bucket_dfs.append(read_bucket_sheet(sheet,excel_bucket))

    buckets = pd.concat(bucket_dfs,ignore_index=True)

    # Remove any entry with a missing acct_num...these will likely be bad skiprows
    # it does remoe any empty sheets however
    buckets.dropna(subset=['Acct_Num'],inplace=True)

    #Check the BUCKETS
    if any(buckets.duplicated(subset=['Acct_Num'])):
        logging.waring('You had a duplicate account number...that doesnt make a lot of sense')
        logging.warning(buckets.loc[buckets.duplicated(subset=['Acct_Num'])])

    # Merge the two together
    all_df = pd.merge(buckets,rpc[['Acct Id Acc','IB_OB','stripped','GC','Agent']],
        how='outer',left_on='Acct_Num',right_on='Acct Id Acc')

    all_df.rename(columns={'Acct_Num':'Queue','Acct Id Acc':'RPC'},inplace=True)

    # all_df.loc[all_df['Associate'] == 'DAGREEN'].dropna(subset=['RPC'])

    rpc_summary_df = rpc_summary(all_df)
    rpc_summary_df.to_csv(output_fn)

# end%%

# %% Logging
def log_uncaught_exceptions(ex_cls, ex, tb):
    logging.critical(''.join(traceback.format_tb(tb)))
    logging.critical('{0}: {1}'.format(ex_cls, ex))

# end%%
# %% argparse overwriting
class argparse_logger(argparse.ArgumentParser):
    def _print_message(self, message, file=None):
        if file is sys.stderr:
            logging.warning('Arg Parse did something bad...see below:')
            logging.error(message)
        else:
            super()._print_message(messag,file=file)

# end%%

# %% Is valid file for arg parser
def is_valid_file(arg):
    if not os.path.exists(arg):
        # parser.error("Cannot find the file: %s" % arg)
        raise argparse.ArgumentError("{0} does not exist".format(arg))
    return arg
# end%%

# %% Read sheets info
def read_bucket_sheet(sheet_name, excel_file,global_names = ['Acct_Num','Delinq','Date'],
    queue_name='Associate'):
    logging.debug('Reading %s'%(sheet_name))
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
        logging.warning('Sheetname %s - We dont have a known pattern...using default')



    df = excel_file.parse(
        sheet_name=sheet_name,skiprows=skiprows,
        converters={cols[0]:str,cols[-1]:pd.to_datetime})


    if not queue_name in df.columns:
        df[queue_name] = np.nan

    df.rename(columns=dict(zip(cols,global_names)),inplace=True)

    df['Bucket'] = sheet_name


    return df[global_names + [queue_name] + ['Bucket']]

# end%%

# %%
def rpc_summary(all_df):
    summary_df = all_df.groupby(['Bucket','Date']).apply(lambda x: pd.Series(dict(
        Queue_total=x['Queue'].nunique(),
        Total_RPC=x['RPC'].count(),
        Unique_RPC=x['RPC'].nunique(),
        Total_PTP=x['RPC'].loc[x['stripped']=='PP'].count(),
        Unique_PTP=x['RPC'].loc[x['stripped']=='PP'].nunique(),
        Outbound_RPC=x['RPC'].loc[x['IB_OB']=='OB'].count(),
        Outbound_PTP=x['RPC'].loc[(x['IB_OB']=='OB') & (x['stripped']=='PP')].count(),
        Inbound_RPC=x['RPC'].loc[x['IB_OB']=='IB'].count(),
        Inbound_PTP=x['RPC'].loc[(x['IB_OB']=='IB') & (x['stripped']=='PP')].count(),
        HD_RPC=x['RPC'].loc[x['GC'] != 'GC'].count(),
        HD_PTP=x['RPC'].loc[(x['GC']!='GC') & (x['stripped']=='PP')].count(),
        HD_OB_RPC=x['RPC'].loc[(x['GC'] != 'GC') & (x['IB_OB'] == 'OB')].count(),
        HD_OB_PTP=x['RPC'].loc[(x['GC']!='GC') & (x['stripped']=='PP') & (x['IB_OB'] == 'OB')].count(),
        HD_IB_RPC=x['RPC'].loc[(x['GC'] != 'GC') & (x['IB_OB'] == 'IB')].count(),
        HD_IB_PTP=x['RPC'].loc[(x['GC']!='GC') & (x['stripped']=='PP') & (x['IB_OB'] == 'IB')].count(),
        GC_RPC=x['RPC'].loc[x['GC'] == 'GC'].count(),
        GC_PTP=x['RPC'].loc[(x['GC']=='GC') & (x['stripped']=='PP')].count(),
        GC_OB_RPC=x['RPC'].loc[(x['GC'] == 'GC') & (x['IB_OB'] == 'OB')].count(),
        GC_OB_PTP=x['RPC'].loc[(x['GC']=='GC') & (x['stripped']=='PP') & (x['IB_OB'] == 'OB')].count(),
        GC_IB_RPC=x['RPC'].loc[(x['GC'] == 'GC') & (x['IB_OB'] == 'IB')].count(),
        GC_IB_PTP=x['RPC'].loc[(x['GC']=='GC') & (x['stripped']=='PP') & (x['IB_OB'] == 'IB')].count(),
        )))

    summary_df['U_RPC_Q'] = summary_df['Unique_RPC'].astype(np.float64)/ summary_df['Queue_total'].astype(np.float64)
    summary_df['U_PTP_Q'] = summary_df['Unique_PTP'].astype(np.float64)/ summary_df['Queue_total'].astype(np.float64)

    cols = [
        'Queue_total',
        'Total_RPC',
        'Unique_RPC',
        'Total_PTP',
        'Unique_PTP',
        'U_RPC_Q',
        'U_PTP_Q',
        'Outbound_RPC',
        'Outbound_PTP',
        'Inbound_RPC',
        'Inbound_PTP',
        'HD_RPC',
        'HD_PTP',
        'HD_OB_RPC',
        'HD_OB_PTP',
        'HD_IB_RPC',
        'HD_IB_PTP',
        'GC_RPC',
        'GC_PTP',
        'GC_OB_RPC',
        'GC_OB_PTP',
        'GC_IB_RPC',
        'GC_IB_PTP']

    summary_df = summary_df[cols]

    return summary_df
# end%%

# %%
if __name__ == '__main__':
    main()
# end%%
