import pytest
from process_reports import read_rpc, process_rpc
import os
import pandas as pd


@pytest.fixture
def good_fn():
    return os.path.join('Sample_Reports', 'ALL_RPC-2-1-2018_Scrubbed.xlsx')


@pytest.fixture
def good_df(good_fn):
    return read_rpc(good_fn)


@pytest.fixture
def good_processed_df(good_df):
    return process_rpc(good_df)


@pytest.mark.parametrize('action_type,num_ib,num_ob', [
    ('CC', 1, 0),
    ('THC', 0, 1),
    ('CC', 1, 0),
    ('THL', 0, 1),
    ('TWC', 0, 1),
    ('OC', 1, 0),
    ('TWL', 0, 1),
    ('TP', 0, 1),
    ('TO', 0, 1),
    ('tO', 0, 1),
    (' tO', 0, 1),
    ('    tO ', 0, 1),
    (' dumb ', 1, 0),
    ])
def test_action_type(good_df, action_type, num_ib, num_ob):
    data = [
        'Acct 1',
        '2/1/2018',
        'MR - Agent 55572',
        action_type,
        'UPAS',
    ]

    df = pd.DataFrame(columns=good_df.columns)
    df.loc[0] = data

    df_processed = process_rpc(df)
    s_processed = df_processed['IB_OB']

    assert len(s_processed.index) == 1
    assert (s_processed == 'IB').sum() == num_ib
    assert (s_processed == 'OB').sum() == num_ob


def test_bad_key(good_df, caplog):
    df = good_df.rename(columns={'Created By Qcc': 'Created By Dummy'})

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        process_rpc(df)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 42
    assert len(caplog.records) == 2
    assert caplog.records[-1].levelname == 'CRITICAL'


def test_type(good_df):
    data = [
        ['Acct 52675', '2018-02-01 7:41:48', 'MR-Agent 55572', 'CC', 'UPAS'],
        ['Acct 2', '2018-02-01 17:57:01', 'GC-Agent 55573', 'THC', 'PP'],
        ['Acct 43813', '2018-02-01 7:03:53', 'GC-Agent 55574', 'CC', 'PP'],
        ['Acct 43814', '2018-02-01 10:47:41', 'GC-Agent 55575', 'THC', 'PP'],
        ['Acct 34', '2018-02-01 8:17:43', 'C1-Agent 55576', 'CC', 'PP'],
        ['Acct 80', '2018-02-01 9:33:08', 'GC-Agent 55577', 'THC', 'PP'],
        ['Acct 51294', '2018-02-01 16:48:49', 'GC-Agent 51849', 'CC', 'UPOT'],
        ['Acct 55034', '2018-02-01 11:36:47', 'GC-Agent 55579', 'THC', 'CB'],
        ['Acct 55034', '2018-02-01 11:36:47', 'gc-Agent 55579', 'THC', 'CB'],
        ['Acct 55034', '2018-02-01 11:36:47', ' gc-Agent 55579', 'THC', 'CB'],
        ['Acct 51294', '2018-02-01 16:48:49', 'CR-Agent 51849', 'CC', 'UPOT'],
        ['Acct 55034', '2018-02-01 11:36:47', 'CR-Agent 55579', 'THC', 'CB'],
        ['Acct 55034', '2018-02-01 11:36:47', 'cr-Agent 55579', 'THC', 'CB'],
        ['Acct 55034', '2018-02-01 11:36:47', ' cr-Agent 55579', 'THC', 'CB']
    ]
    df = pd.DataFrame(data, columns=good_df.columns)

    df_p = process_rpc(df)

    assert len(df_p.index) == len(data)
    assert df_p['GC'].sum() == 8
    assert df_p['CR'].sum() == 4
    assert df_p['HD'].sum() == 2
