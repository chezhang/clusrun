#Requires -RunAsAdministrator

[CmdletBinding(DefaultParametersetName="default")]  
Param(
    [string] $headnodes = "localhost", 
    [string] $location = "C:\Program Files\clusrun",
    [Parameter(ParameterSetName = "install")]
    [switch] $reinstall = $false,
    [Parameter(ParameterSetName = "uninstall")]
    [switch] $uninstall = $false
)

"$(Get-Date)  Setup clusrun: headnodes=$headnodes, location=$location, reinstall=$reinstall, uninstall=$uninstall"

$setup_url = "https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.zip"
$setup_file = "$pwd\clusrun.setup.zip"

if($uninstall -or $reinstall) {
    "$(Get-Date)  Uninstall clusrun in $location"
    Set-Location $location
    .\uninstall.bat
    if($uninstall) {
        return
    }
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
for ($i = 0; $i -le 10; $i++) {
    "$(Get-Date)  Download clusrun from $setup_url"
    (New-Object System.Net.WebClient).DownloadFile($setup_url, $setup_file)
    if([System.IO.File]::Exists($setup_file)) { break } else { Start-Sleep $(Get-Random -Maximum 5) }
}

"$(Get-Date)  Extract clusrun from $setup_file to $location"
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory($setup_file, $location)
Remove-Item $setup_file

"$(Get-Date)  Install clusrun"
Set-Location $location
.\install.bat
Remove-Item install.bat
Start-Sleep 1

"$(Get-Date)  Set headnodes to $headnodes"
.\clusnode.exe set -headnodes "$headnodes"