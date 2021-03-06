; Script generated by the Inno Setup Script Wizard.
; SEE THE DOCUMENTATION FOR DETAILS ON CREATING INNO SETUP SCRIPT FILES!

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
; (To generate a new GUID, click Tools | Generate GUID inside the IDE.)
AppId={{78F004A2-0A28-4E7F-BD83-0ED27E663582}
AppName=RPC Processing
AppVersion=0.50
AppPublisher=Optimal Analytics LLC.
AppPublisherURL=https://github.com/eskemojoe007/call_center_reports
AppSupportURL=https://github.com/eskemojoe007/call_center_reports
AppUpdatesURL=https://github.com/eskemojoe007/call_center_reports
DefaultDirName={pf}\RPC Processing
DisableDirPage=yes
DefaultGroupName=Optimal Analytics
AllowNoIcons=yes
LicenseFile=C:\Users\212333077\Documents\GitHub\call_center_reports\LICENSE
OutputBaseFilename=RPC_Setup
Compression=lzma
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64
OutputDir=.\dist

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "dist\RPC Processing.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "dist\process_reports.log"; DestDir: "{localappdata}\RPCProcessing"; Flags: onlyifdoesntexist uninsneveruninstall
; NOTE: Don't use "Flags: ignoreversion" on any shared system files

[Icons]
Name: "{group}\RPC Processing"; Filename: "{app}\RPC Processing.exe"
; Remove uninstall from start menu
Name: "{group}\RPC Log File"; Filename: "{localappdata}\RPCProcessing\process_reports.log"
; Name: "{group}\{cm:UninstallProgram,RPC Processing}"; Filename: "{uninstallexe}"
Name: "{commondesktop}\RPC Processing"; Filename: "{app}\RPC Processing.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\RPC Processing.exe"; Description: "{cm:LaunchProgram,RPC Processing}"; Flags: nowait postinstall skipifsilent
