;--------------------------------
;Include Modern UI

  !include "MUI2.nsh"

;--------------------------------
;General

  ;Name and file
  Name "PgManage"
  OutFile "pgmanage_setup.exe"
  Unicode True

  ;Default installation folder
  InstallDir "$LOCALAPPDATA\$(^Name)"

  ;Get installation folder from registry if available
  InstallDirRegKey HKCU "Software\$(^Name)" ""

  ;Request application privileges for Windows Vista
  RequestExecutionLevel user
  ManifestDPIAware true
;--------------------------------
;Interface Configuration

  !define MUI_HEADERIMAGE
  !define MUI_HEADERIMAGE_BITMAP "install_header.bmp" ; optional
  !define MUI_ICON win-icon.ico
  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_LICENSE "license.txt"
  !insertmacro MUI_PAGE_COMPONENTS
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_INSTFILES

  !define MUI_FINISHPAGE_RUN "$InstDir\pgmanage-app.exe"
  !define MUI_FINISHPAGE_RUN_TEXT "Run PgManage"
  !insertmacro MUI_PAGE_FINISH

  !insertmacro MUI_UNPAGE_COMPONENTS
  !insertmacro MUI_UNPAGE_INSTFILES

;--------------------------------
;Languages

  !insertmacro MUI_LANGUAGE "English"

;--------------------------------
;Installer Sections

Section "Program" AppFiles
  SectionIn RO

  ;Add check for directory existence and delete if it exists
  IfFileExists "$INSTDIR\pgmanage-server" 0 +2
    RMDir /r "$INSTDIR\pgmanage-server"


  SetOutPath "$INSTDIR"

  File /r ${SRCDIR}\*.*

  ;Store installation folder
  WriteRegStr HKCU "Software\$(^Name)" "" $INSTDIR

  ;Create uninstaller
  WriteUninstaller "$INSTDIR\Uninstall.exe"

SectionEnd


Section "Start menu shortcut"
  CreateShortcut "$SMPrograms\$(^Name).lnk" "$InstDir\pgmanage-app.exe"
SectionEnd

Section "Desktop shortcut"
  CreateShortcut "$Desktop\$(^Name).lnk" "$InstDir\pgmanage-app.exe"
SectionEnd


;--------------------------------
;Descriptions

  ;Language strings
  LangString DESC_AppFiles ${LANG_ENGLISH} "Application Files."

  ;Assign language strings to sections
  !insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
    !insertmacro MUI_DESCRIPTION_TEXT ${AppFiles} $(DESC_AppFiles)
  !insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Uninstaller Section

Section "un.Application Files"
  SectionIn RO
  Delete "$INSTDIR\Uninstall.exe"

  RMDir /r "$INSTDIR"

  DeleteRegKey /ifempty HKCU "Software\$(^Name)"
  Delete "$SMPROGRAMS\$(^Name).lnk"
  Delete "$Desktop\$(^Name).lnk"
SectionEnd

Section "un.Application Data"
  RMDir /r "$PROFILE\.pgmanage"
SectionEnd