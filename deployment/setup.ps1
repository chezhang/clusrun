[CmdletBinding(DefaultParametersetName="default")]  
Param(
    [string] $headnodes = "localhost", 
    [string] $installDir = "C:\clusrun",
    [Parameter(ParameterSetName = "install")]
    [switch] $reinstall = $false,
    [Parameter(ParameterSetName = "uninstall")]
    [switch] $uninstall = $false
)

$download_location = "https://github.com/chezhang/clusrun/releases/download/0.1.0"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if($uninstall -or $reinstall) {
    $uninstallScript = "$installDir\uninstall.bat"
    if(![System.IO.File]::Exists($uninstallScript)) {
        (New-Object System.Net.WebClient).DownloadFile("$download_location/uninstall.bat", "$uninstallScript")
    }
    cd $installDir
    .\uninstall.bat
    if($uninstall) {
        return
    }
}

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force $installDir
(New-Object System.Net.WebClient).DownloadFile("$download_location/clusnode.exe", "$installDir\clusnode.exe")
(New-Object System.Net.WebClient).DownloadFile("$download_location/install.bat", "$installDir\install.bat")
(New-Object System.Net.WebClient).DownloadFile("$download_location/uninstall.bat", "$installDir\uninstall.bat")

cd $installDir
.\install.bat
rm install.bat
.\clusnode.exe set -headnodes "$headnodes"