# Call Center Reports
[![Build status](https://ci.appveyor.com/api/projects/status/qx885ewwflto9m7b?svg=true)](https://ci.appveyor.com/project/eskemojoe007/call-center-reports)

This project installs and performs custom RPC processing to combine different datasets
.  

See https://docs.google.com/document/d/1ZflzGm4oWcy1sGBahKNfrDukEMecpg4nqnDXtTCZTYY/edit?usp=sharing

## General User Instructions

### Installation  
1. Head over to https://github.com/eskemojoe007/call_center_reports/releases and download the latest RPC_Setup.exe to install
   - Your browser may warn that this is a dangerous file - It is not, you are free to look at the source code.
2. Run the installer locally like you would any other application

### Usage of the Application
After installation, the application is accessible through the desktop icon, or start menu
like any other application.

Running the program will bring up the following window the first time:

![image](https://user-images.githubusercontent.com/22135005/45060999-79bc2800-b070-11e8-97ff-cd24711bb366.png)

Every time you use it you must specify an RPC File, Bucket File, and the location you
want the output stored in (this is remembered after the first go.)

![image](https://user-images.githubusercontent.com/22135005/45061068-d1f32a00-b070-11e8-8dd7-b3aca3f65dd4.png)

Clicking start will then kick off the process.  When done, check the output files.

### Checking the logs
If there is an error, logs are kept in the `%localappdata%` folder, which is confusing to get to.
So, there is a shortcut in the start menu

![image](https://user-images.githubusercontent.com/22135005/45061522-d7ea0a80-b072-11e8-95db-528aa420bba1.png)

This log will help the developers debug what went wrong.  

## Developer Instructions

### Cloning, installing, running, and compiling
installing `pipenv install --dev` in the local repository should work well.
It does rely on `colored_logger` (see https://github.com/eskemojoe007/colored_logger).

Creating install binaries
1. `pipenv run pyinstaller process_reports.spec`
2. Install Inno Setup
3. Compile `create_installer.iss` using inno setup

### Running test suite
`pipenv run python -m pytest` - will run the entire test suite

We also leverage appveyor to run tests and build binaries.  

### Command line
Whether using `pipenv run python process_reports.py` or just `process_reports.exe`
in the command line is acceptable.  If you use no inputs, it will open up the dialogues just
like double clicking does.  You can specify the rpc and bucket files using the `-r` and `-b` flag respectively.

Example:

`dist\process_reports.exe -r Sample_Reports\ALL_RPC-2-1-2018_Scrubbed.xlsx -b Sample_Reports\2_1_18Bucket_Scrubbed.xls -d output_folder`

You can also specify how the output tag looks.  If you want a different date or header just  use
`-o <header>`

You can always use `--help` to see the inputs.
