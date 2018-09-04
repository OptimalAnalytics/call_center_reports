import pytest
from process_reports import sub_script, RPCArgParse
import os
import pandas as pd


@pytest.fixture
def rpc_fn(request):
    if request.param == 1:
        return os.path.join('Sample_Reports', 'ALL_RPC-2-1-2018_Scrubbed.xlsx')
    elif request.param == 2:
        return os.path.join('Sample_Reports', 'ALL_RPC-7-3_2018_Scrubbed.xlsx')


@pytest.fixture
def bucket_fn(request):
    if request.param == 1:
        return os.path.join('Sample_Reports', '2_1_18Bucket_Scrubbed.xls')
    elif request.param == 2:
        return os.path.join('Sample_Reports', 'Daily_Queues_by_Bucket.7.3.18_Scrubbed.xls')


@pytest.mark.parametrize('rpc_fn,bucket_fn,header', [
    (1, 1, 'Sample1'),
    (2, 2, 'Sample2'),
    ], indirect=['rpc_fn', 'bucket_fn'])
def test_default(rpc_fn, bucket_fn, header):
    # Run the main
    parser = RPCArgParse()

    args = parser.parse_args(['-r', rpc_fn, '-b', bucket_fn, '-o', 'test_out', '-d', 'tests'])
    sub_script(args)

    # Read in the files to ensure it is working
    good = header

    files = ['Agent_Summary.csv', 'Queue_Summary.csv', 'RPC_Summary.csv']

    for file in files:
        good_df = pd.read_csv(os.path.join('tests', '{}_{}'.format(good, file)))

        out_file = os.path.join('tests', '{}_{}'.format('test_out', file))
        test_df = pd.read_csv(out_file)

        assert (good_df == test_df).all().all()
        os.remove(out_file)
