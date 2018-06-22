import pytest
from process_reports import read_rpc, check_excel, check_extension, read_info
import os

good_fn = os.path.join('Sample_Reports', 'ALL_RPC-2-1-2018_Scrubbed.xlsx')


@pytest.mark.parametrize("fn,acceptable,case,expected", [
    ('cool.txt', ['txt', 'xls'], True, True),
    ('cool.ini', ['txt', 'xls'], True, False),
    ('C:\monster\cool.ini', ['txt', 'xls'], True, False),
    ('C:\monster\cool.txt', ['txt', 'xls'], True, True),
    ('C:\monster\cool.xls', ['txt', 'xls'], True, True),
    ('C:\monster\cool.XLS', ['txt', 'xls'], True, True),
    ('C:\monster\cool.XLS', ['txt', 'xls'], False, False),
    ('C:\monster\cool.xls', ['txt', 'XLS'], False, False),
    ('C:\monster\cool.xls', ['txt', 'XLS'], True, True),
    ('C:\monster\cool.ini', ['txt', 'XLS'], True, False),
    ('C:\monster\cool.ini', ['txt', 'XLS'], False, False),
])
def test_check_extensions(fn, acceptable, case, expected):
    assert check_extension(fn, acceptable, case_insensitive=case) == expected


@pytest.mark.parametrize("fn,expected", [
    ('cool.xls', True),
    ('cool.xlsx', True),
    ('cool.XLSX', True),
    ('cool.xlsm', True),
    ('cool.xlsb', True),
    ('cool.csv', False),
    ('cool.ini', False),
])
def test_check_excel(fn, expected):
    assert check_excel(fn) == expected


@pytest.mark.parametrize("f_type", ['csv', 'ini'])
def test_bad_f_types(caplog, f_type):
    with pytest.raises(NotImplementedError):
        read_info(good_fn, f_type=f_type)
    for record in caplog.records:
        assert record.levelname == 'ERROR'
    assert 'f_type' in caplog.text
    assert 'supported' in caplog.text
    assert f_type in caplog.text


def test_bad_f_type_file(caplog):
    with pytest.raises(ValueError):
        read_info('cool.csv')
    for record in caplog.records:
        assert record.levelname == 'ERROR'


def test_good_file():
    df = read_rpc(good_fn)
    assert 'Created By Qcc' in df.columns.values
    assert 'Acct Id Acc' in df.columns.values


def test_kwargs():
    df = read_rpc(good_fn, usecols=2)
    assert 'Call Result Type Qcc' not in df.columns.values
