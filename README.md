# Call Center Reports
This project is designed to showcase how pandas and pyinstaller can be used
to drastically reduce processing time of call center reporting.

See https://docs.google.com/document/d/1ZflzGm4oWcy1sGBahKNfrDukEMecpg4nqnDXtTCZTYY/edit?usp=sharing

## Installation
Installation can be simple or complex depending on how you want to use it.

### Cloning, installing, running, and compiling
installing `pipenv install --dev` in the local repository should work well, You'll likely want to specify a 32 bit python as well using `--python <path_to_32_bit>`
It does rely on `colored_logger` (see https://github.com/eskemojoe007/colored_logger) installed as a local package.  (You will need to clone this repo and edit the pipfile to match)

Creating binaries
`pipenv run pyinstaller process_reports.spec`

### Download the Release
Head over to https://github.com/eskemojoe007/call_center_reports/releases and download the latest .exe


## Using the code
We tried to design the code to be run in multple ways to make it easier.

### Double Click
The exe can be double clicked.  It will prompt you to pick the proper RPC and Bucket
files, and then process them.  It will save the output files in its location
named with the current days date.

### Command line
Whether using `pipenv run python process_reports.py` or just `process_reports.exe`
in the command line is acceptable.  If you use no inputs, it will open up the dialogues just
like double clicking does.  You can specify the rpc and bucket files using the `-r` and `-b` flag respectively.


Example:

`dist\process_reports.exe -r Sample_Reports\ALL_RPC-2-1-2018_Scrubbed.xlsx -b Sample_Reports\2_1_18Bucket_Scrubbed.xls`

You can also specify how the output tag looks.  If you want a different date or header just  use
`-o <header>`

You can always use `--help` to see the inputs.
