sc stop clusnode
sc delete clusnode
sc create clusnode binpath= "%~dp0clusnode.exe start" start= auto
sc start clusnode