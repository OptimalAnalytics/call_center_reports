# Katie Reports
This project is testing and using everything katie needs to process agent and queue performance

## Designed with 32 bit windows

## Creating binaries
`pipenv run pyinstaller process_reports.spec`

##Running from binary
`dist\process_reports.exe Sample_Reports\ALL_RPC-2-4-2018.xlsx Sample_Reports\2_4_18Buckets.xls`

## Running from normal
`pipenv run python process_reports.py Sample_Reports\ALL_RPC-2-4-2018.xlsx Sample_Reports\2_4_18Buckets.xls`
