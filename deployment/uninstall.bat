@echo off

sc stop clusnode
sc delete clusnode

IF /I "%1"=="-cleanup" (
    rmdir /Q /S "%~dp0clusnode.exe.db"
    rmdir /Q /S "%~dp0clusnode.exe.logs"
    del "%~dp0clusnode.exe.config"
)

ping 127.0.0.1 -n 2 > nul
del "%~dp0clusnode.exe"
del "%~dp0clus.exe"

IF /I "%1"=="-cleanup" (
    ( del /q /f "%~f0" >nul 2>&1 & exit /b 0 )
)