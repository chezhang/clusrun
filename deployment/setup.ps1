#Requires -RunAsAdministrator

[CmdletBinding(DefaultParametersetName="default")]  
Param(
    [string] $headnodes = "localhost",
    [int] $port = 50505,
    [string] $location = "C:\Program Files\clusrun",
    [string] $setup_url = "https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.zip",
    [Parameter(ParameterSetName = "install")]
    [switch] $reinstall = $false,
    [Parameter(ParameterSetName = "uninstall")]
    [switch] $uninstall = $false,
    [string] $cert_file,
    [string] $key_file,
    [string] $cert_base64,
    [string] $key_base64
)

"$(Get-Date)  Setup clusrun: headnodes=$headnodes, location=$location, setup_url=$setup_url, reinstall=$reinstall, uninstall=$uninstall, cert_file=$cert_file, key_file=$key_file, cert_base64=$cert_base64, key_base64=$key_base64"

if (-not $uninstall) {
    if (-not $cert_file -and -not $cert_base64) {
        "Please specify the cert for secure communication."
        return
    }

    if (-not $key_file -and -not $key_base64) {
        "Please specify the key for secure communication."
        return
    }
}

if ($uninstall -or $reinstall) {
    "$(Get-Date)  Uninstall clusrun in $location"
    & "$location\uninstall.bat" -cleanup
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

$cert = "$location\cert.pem"
$key = "$location\key.pem"

if ($cert_base64) {
    "$(Get-Date)  Create cert file $cert"
    [System.IO.File]::WriteAllBytes($cert, [System.Convert]::FromBase64String($cert_base64))
}

if ($key_base64) {
    "$(Get-Date)  Create key file $key"
    [System.IO.File]::WriteAllBytes($key, [System.Convert]::FromBase64String($key_base64))
}

if ($cert_file) {
    "$(Get-Date)  Copy cert file $cert_file to $cert"
    Copy-Item $cert_file $cert
}

if ($key_file) {
    "$(Get-Date)  Copy key file $key_file to $key"
    Copy-Item $key_file $key
}

$ErrorActionPreference = "Continue"
ForEach($file in @($cert, $key)) {
    if (Test-Path $file) {
        "$(Get-Date)  Set ACL of $file"
        $acl = Get-Acl $file
        $acl.SetAccessRuleProtection($true, $true)
        $acl | Set-Acl $file
        $acl = Get-Acl $file
        ForEach($rule in $acl.Access) { 
            if ($rule.IdentityReference -ne "NT AUTHORITY\SYSTEM" -and $rule.IdentityReference -ne "BUILTIN\Administrators" ) {
                "Remove $($rule.IdentityReference)"
                $acl.RemoveAccessRule($rule)
            }
        }
        $acl | Set-Acl $file
        Get-Acl $file | Format-List
    }
}
$ErrorActionPreference = "Stop"

"$(Get-Date)  Install clusrun"
& "$location\install.bat" $port
Remove-Item "$location\install.bat"
Start-Sleep 1

"$(Get-Date)  Set headnodes to $headnodes"
clusnode config set -headnodes "$headnodes" -node "localhost:$port"

"$(Get-Date)  Clusrun is installed in $location"