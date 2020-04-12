@echo off

IF "%1"=="" ( SET "port=50505" ) ELSE ( SET "port=%1" )

sc stop clusnode >nul
sc delete clusnode >nul
sc create clusnode binpath= "%~dp0clusnode.exe start -host localhost:%port%" start= auto
sc start clusnode