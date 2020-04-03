#Requires -RunAsAdministrator

[CmdletBinding(DefaultParametersetName="default")]  
Param(
    [string] $headnodes = "localhost",
    [int] $port = 50505,
    [string] $location = "C:\Program Files\clusrun",
    [string] $setup_url = "https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.zip",
    [Parameter(ParameterSetName = "install")]
    [switch] $reinstall = $false,
    [Parameter(ParameterSetName = "uninstall")]
    [switch] $uninstall = $false
)

"$(Get-Date)  Setup clusrun: headnodes=$headnodes, location=$location, setup_url=$setup_url, reinstall=$reinstall, uninstall=$uninstall"

if ($uninstall -or $reinstall) {
    "$(Get-Date)  Uninstall clusrun in $location"
    & "$location\uninstall.bat"
    if ($uninstall) {
        return
    }
}

if ($setup_url.StartsWith("http", "CurrentCultureIgnoreCase")) {
    $setup_file = "$pwd\clusrun.setup.zip"
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    for ($i = 0; $i -le 10; $i++) {
        "$(Get-Date)  Download clusrun from $setup_url"
        (New-Object System.Net.WebClient).DownloadFile($setup_url, $setup_file)
        if ([System.IO.File]::Exists($setup_file)) { break } else { Start-Sleep $(Get-Random -Maximum 5) }
    }
    $setup_url = $setup_file
}

"$(Get-Date)  Extract clusrun from $setup_url to $location"
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory($setup_url, $location)

"$(Get-Date)  Install clusrun"
Set-Location $location
.\install.bat $port
Remove-Item install.bat
Start-Sleep 1

"$(Get-Date)  Set headnodes to $headnodes"
.\clusnode.exe config set -headnodes "$headnodes" -node "localhost:$port"