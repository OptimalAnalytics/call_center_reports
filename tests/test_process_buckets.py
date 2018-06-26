import pytest
from process_reports import read_buckets, check_bucket_duplicates
import os


@pytest.fixture
def good_bucket():
    return read_buckets(os.path.join('Sample_Reports', '2_1_18Bucket_Scrubbed.xls'))


def test_bad_key(good_bucket, caplog):
    df = good_bucket.copy()
    df.loc['Acct_Num', -1] = df['Acct_Num'].iloc[-2]

    # with pytest.raises(SystemExit) as pytest_wrapped_e:
    check_bucket_duplicates(df)
    # assert pytest_wrapped_e.type == SystemExit
    # assert pytest_wrapped_e.value.code == 42
    assert len(caplog.records) == 2
    assert caplog.records[-1].levelname == 'WARNING'
