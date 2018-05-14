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
    ''' We expect this to pass using xpass with a good filename
    https://docs.pytest.org/en/documentation-restructure/how-to/skipping.html'''
    rpcargparse.parse_args(['-b',bucket_fn_data[0][0]])

def test_bad_bucket(rpcargparse):
    '''We expect a bad one to SystemExit with a value of 2 when a bad
    file is used
    https://medium.com/python-pandemonium/testing-sys-exit-with-pytest-10c6e5f7726f'''
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        rpcargparse.parse_args(['-b',bucket_fn_data[1][0]])

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_output_default_val1(rpcargparse):
    args = rpcargparse.parse_args()
    assert args.output_fn == '%s'%datetime.date.today().strftime('%Y_%m_%d')


def test_output_default_val2(rpcargparse):
    '''We expect a bad one to SystemExit with a value of 2 when a bad
    file is used'''
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        rpcargparse.parse_args(['-o'])

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_output_default_val3(rpcargparse):
    args = rpcargparse.parse_args(['-o','cool'])
    assert args.output_fn == 'cool'

def test_output_default_val4(rpcargparse):
    args = rpcargparse.parse_args(['-o',''])
    print(repr(args.output_fn))
    assert args.output_fn == ''

# @pytest.mark.parametrize("fn,is_file",bucket_fn_data)
# def test_bucket_input(fn,is_file,rpcargparse):
#     if not is_file:
#         with pytest.raises(ArgumentError):
#             rpcargparse.parse_args(['-b',fn])

# @pytest.mark.parametrize("fn,is_file",bucket_fn_data)
# def test_bucket_input1(fn,is_file,rpcargparse):
#     if not is_file:
#         with pytest.raises(ArgumentTypeError):
#             rpcargparse.parse_args(['--bucket_fn',fn])
