import pytest
from process_reports import read_buckets, read_bucket_sheet
import os
import pandas as pd
import logging
import numpy as np

good_bucket = os.path.join('Sample_Reports', '2_1_18Bucket_Scrubbed.xls')
mixed_bucket = os.path.join('tests', '2_1_18Bucket_Scrubbed_Tester_sheets.xls')


@pytest.fixture
def good_excel():
    ''' Returns excel file object for known good bucket'''
    return pd.ExcelFile(good_bucket)


@pytest.fixture
def mixed_excel():
    ''' Returns excel file object for known test bucket'''
    return pd.ExcelFile(mixed_bucket)


def test_non_standard_sheet(caplog, mixed_excel):
    caplog.set_level(logging.WARNING, logger='report')
    df = read_bucket_sheet('Tester_CAN31', mixed_excel)
    for record in caplog.records:
        assert record.levelno >= logging.WARNING
    assert 'Sheetname Tester_CAN31' in caplog.text
    assert 'pattern' in caplog.text
    assert np.issubdtype(df['Date'], np.datetime64)
    assert all(df['Bucket'] == 'Tester_CAN31')


def test_bad_col_name(caplog, mixed_excel):
    caplog.set_level(logging.WARNING, logger='report')
    with pytest.raises(KeyError):
        read_bucket_sheet('Tester_CAN31_Bad_Col_name', mixed_excel)

    for record in caplog.records:
        assert record.levelno >= logging.WARNING
    assert 'specified col' in caplog.text
    assert 'Current Dat' in caplog.text


@pytest.mark.parametrize('sheetname', [
    '60 Day',
    'EPD 31+',
    'FPD 2-30',
])
def test_empty_sheets(sheetname, caplog, mixed_excel):
    caplog.set_level(logging.WARNING, logger='report')

    read_bucket_sheet(sheetname, mixed_excel)

    for record in caplog.records:
        assert record.levelno >= logging.WARNING
    assert 'no account numbers' in caplog.text


def test_bad_date_format(caplog, mixed_excel):
    caplog.set_level(logging.WARNING, logger='report')
    df = read_bucket_sheet('Tester_CAN31_dateformat', mixed_excel)
    for record in caplog.records:
        assert record.levelno >= logging.WARNING
    assert 'Read buckets failed' in caplog.text
    assert np.issubdtype(df['Date'], np.datetime64)


def test_no_date_format(caplog, mixed_excel):
    caplog.set_level(logging.WARNING, logger='report')
    with pytest.raises(ValueError):
        read_bucket_sheet('Tester_CAN31_no_date', mixed_excel)
    for record in caplog.records:
        assert record.levelno >= logging.WARNING
    assert 'Read buckets failed' in caplog.text


def test_working_file():
    df = read_buckets(good_bucket)
    assert len(df.loc[df['Bucket'] == 'Can 31+'].index) > 1
    assert np.issubdtype(df['Date'], np.datetime64)
    # assert df['Date'].dtype == np.datetime64
    # assert isinstance(df['Date'],np.datetime64)
