[Setup]
AppName=Family Budget Tracker
AppVersion=1.0
AppPublisher=Family Budget
DefaultDirName={autopf}\FamilyBudgetTracker
DefaultGroupName=Family Budget Tracker
UninstallDisplayIcon={app}\expense-tracker.exe
OutputDir=installer
OutputBaseFilename=FamilyBudgetTracker-Setup
Compression=lzma2
SolidCompression=yes
SetupIconFile=app.ico
WizardStyle=modern
PrivilegesRequired=lowest

[Languages]
Name: "hebrew"; MessagesFile: "compiler:Languages\Hebrew.isl"

[Files]
Source: "dist\expense-tracker.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Family Budget Tracker"; Filename: "{app}\expense-tracker.exe"
Name: "{group}\Uninstall Family Budget Tracker"; Filename: "{uninstallexe}"
Name: "{autodesktop}\Family Budget Tracker"; Filename: "{app}\expense-tracker.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create desktop shortcut"; GroupDescription: "Additional icons:"

[Run]
Filename: "{app}\expense-tracker.exe"; Description: "Launch Family Budget Tracker"; Flags: nowait postinstall skipifsilent
