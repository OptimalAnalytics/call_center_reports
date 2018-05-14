import pytest
from process_reports import RPCArgParse
import os
from argparse import ArgumentTypeError, ArgumentError
import datetime

@pytest.fixture
def rpcargparse():
    '''Returns a typical RPCArgParse'''
    return RPCArgParse()

@pytest.fixture
def descrip_RPCArgParse():
    '''Returns a typical RPCArgParse'''
    return RPCArgParse(description='Alternate descrip')


def test_RPCArgParse_descrip1(rpcargparse):
    assert rpcargparse.description == 'Read in the main files to create RPC summary'
def test_RPCArgParse_descrip2(descrip_RPCArgParse):
    assert descrip_RPCArgParse.description == 'Alternate descrip'


bucket_fn_data = [
    (os.path.join('Sample_Reports','2_1_18Bucket_Scrubbed.xls'),True),
    (os.path.join('Sample_Reports','2_1_18Bucket_BAD.xls'),False),
]

@pytest.mark.parametrize("fn,is_file",bucket_fn_data)
def test_is_valid_file(fn,is_file,rpcargparse):
    '''
    Test the is_valid_file with the paraemtrized inputs.
    Should raise an error for a bad file and nothing for a good file.
    '''
    if not is_file:
        with pytest.raises(ArgumentTypeError):
            rpcargparse.is_valid_file(fn)

@pytest.mark.xpass(strict=True)
def test_good_bucket(rpcargparse):
    '''
    We expect this to pass using xpass with a good filename
    https://docs.pytest.org/en/documentation-restructure/how-to/skipping.html
    '''
    rpcargparse.parse_args(['-b',bucket_fn_data[0][0]])

@pytest.mark.parametrize('parse',[['-b',bucket_fn_data[1][0]],['-o']])
def test_bad_arg(parse,rpcargparse):
    '''
    These are tests we expect to fail, bad file, bad syntax etc.
    https://medium.com/python-pandemonium/testing-sys-exit-with-pytest-10c6e5f7726f
    '''
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        rpcargparse.parse_args(parse)

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

@pytest.mark.parametrize("parse,key,expected",[
    (None,'output_fn','%s'%datetime.date.today().strftime('%Y_%m_%d')),
    (['-o','cool'],'output_fn','cool'),
    (['-o',''],'output_fn',''),
])
def test_output_default_val(parse,key,expected,rpcargparse):
    args = rpcargparse.parse_args(parse)
    assert vars(args).get(key) == expected
