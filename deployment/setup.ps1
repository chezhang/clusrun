[CmdletBinding(DefaultParametersetName="default")]  
Param(
    [string] $headnodes = "localhost", 
    [string] $installDir = "C:\clusrun",
    [Parameter(ParameterSetName = "install")]
    [switch] $reinstall = $false,
    [Parameter(ParameterSetName = "uninstall")]
    [switch] $uninstall = $false
)

$setup_url = "https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.zip"
$setup_file = "$pwd\clusrun.setup.zip"

if($uninstall -or $reinstall) {
    Set-Location $installDir
    .\uninstall.bat
    if($uninstall) {
        return
    }
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
for ($i = 0; $i -le 10; $i++) {
    (New-Object System.Net.WebClient).DownloadFile($setup_url, $setup_file)
    if([System.IO.File]::Exists($setup_file)) { break } else { Start-Sleep $(Get-Random -Maximum 5) }
}

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory($setup_file, $installDir)
Remove-Item $setup_file

Set-Location $installDir
.\install.bat
Remove-Item install.bat
.\clusnode.exe set -headnodes "$headnodes"