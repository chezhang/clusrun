Param(
    [Parameter(Mandatory = $true)]
    [string] $resourceGroup, 
    [Parameter(Mandatory = $true)]
    [string] $vmssName,
    [string] $headnodes,
    [switch] $uninstall = $false,
    [string] $cert_file,
    [string] $key_file
)

$ErrorActionPreference = "Stop"

$baseUrl = "https://github.com/chezhang/clusrun/releases/download/v0.2.0"
$vmssExtensionName = "Install_clusrun"
$installParameter = ""
$cert_base64 = ""
$key_base64 = ""

if (-not $uninstall) {
    if (-not $cert_file -or -not $key_file) {
        "Certificate file or key file is not specified, generate them."
        $cert_file = "$pwd\cert.pem"
        $key_file = "$pwd\key.pem"
        if ([System.IO.File]::Exists($cert_file)) {
            "Certificate file $cert_file already exists, stop."
            return
        }

        if ([System.IO.File]::Exists($key_file)) {
            "Key file $key_file already exists, stop."
            return
        }

        $openssl_url = "$baseUrl/openssl.zip"
        $openssl_file = "$pwd\clusrun_openssl.zip"
        $openssl_location = "$pwd\clusrun_openssl"
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        "$(Get-Date)  Download openssl from $openssl_url"
        (New-Object System.Net.WebClient).DownloadFile($openssl_url, $openssl_file)
        "$(Get-Date)  Extract openssl from $openssl_file to $openssl_location"
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        [System.IO.Compression.ZipFile]::ExtractToDirectory($openssl_file, $openssl_location)
        & $openssl_location\openssl.exe req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout $key_file -out $cert_file -subj "/C=/ST=/L=/O=/OU=/CN="
        Remove-Item $openssl_location -Recurse
        Remove-Item $openssl_file
    }

    $cert_file = (Resolve-Path $cert_file).Path
    "Use certificate file $cert_file"
    $cert_base64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes($cert_file))
    $key_file = (Resolve-Path $key_file).Path
    "Use key file $key_file"
    $key_base64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes($key_file))
}

$vmss = Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -ErrorAction Stop
$extensions = (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions

$windows = $vmss.VirtualMachineProfile.OsProfile.WindowsConfiguration
$linux = $vmss.VirtualMachineProfile.OsProfile.LinuxConfiguration

if ($windows -and $linux -or !$windows -and !$linux) {
    "Unknown OS type"
    $windows
    $linux
    return
}

if (!$headnodes -and !$uninstall) {
    $firstInstance = (Get-AzVmssVM -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName)[0]
    if (!$firstInstance) {
        "Can not get the first instance in VMSS"
        return
    }
    $name = $firstInstance.Name
    $headnodes = $firstInstance.OsProfile.ComputerName
    "Use the first instance $name with hostname $headnodes as headnode"
}

"`r`n`[$(Get-Date)] Current extensions:"
$extensions

$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
if ($clusrun) {
    if ($windows) {
        $installParameter = "-reinstall"
    } else {
        $installParameter = "-r"
    }
    $clusrun.StatusesSummary | Format-Table
    
    "`r`n`r`n[$(Get-Date)] Removing extension $vmssExtensionName ..."
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $vmssExtensionName | Out-Null
    Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
    Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null
    
    "`r`n`r`n[$(Get-Date)] Current extensions:"
    (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions | Format-Table
}

if ($uninstall) {
    $vmssExtensionName = "Uninstall_clusrun"
    $uninstall_clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName}
    if ($uninstall_clusrun) {
        "`r`n`r`n[$(Get-Date)] Removing extension $vmssExtensionName ..."
        Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $vmssExtensionName | Out-Null
        Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
        Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null
        
        "`r`n`r`n[$(Get-Date)] Current extensions:"
        (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions | Format-Table
    }

    if (!$clusrun) {
        "No clusrun to uninstall"
        return
    }

    if ($windows) {
        $installParameter = "-uninstall"
    } else {
        $installParameter = "-u"
    }
}

"`r`n`r`n[$(Get-Date)] Adding extension $vmssExtensionName ..."

if ($windows) {
    $vmss = Add-AzVmssExtension `
        -VirtualMachineScaleSet $vmss `
        -Name "$vmssExtensionName" `
        -Publisher "Microsoft.Compute" `
        -Type "CustomScriptExtension" `
        -TypeHandlerVersion 1.9 `
        -Setting @{
            "fileUris" = ("$baseUrl/setup.zip","$baseUrl/setup.ps1");
            "commandToExecute" = "powershell -ExecutionPolicy Unrestricted -File setup.ps1 $installParameter -headnodes `"$headnodes`" -setup_url setup.zip -cert_base64 `"$cert_base64`" -key_base64 `"$key_base64`" >`"%cd%\clusrun.setup.log`" 2>&1"
            }
} else {
    $vmss = Add-AzVmssExtension `
        -VirtualMachineScaleSet $vmss `
        -Name "$vmssExtensionName" `
        -Publisher "Microsoft.Azure.Extensions" `
        -Type "CustomScript" `
        -TypeHandlerVersion 2.1 `
        -Setting @{
            "fileUris" = ("$baseUrl/setup.tar.gz","$baseUrl/setup.sh");
            "commandToExecute" = "bash setup.sh $installParameter -h `"$headnodes`" -s setup.tar.gz -e `"$cert_base64`" -y `"$key_base64`""
            }
}

Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null

"`r`n`r`n[$(Get-Date)] Current extensions:"
$extensions = (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions
$extensions | Format-Table
$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
$clusrun.StatusesSummary | Format-Table

if ($uninstall) {
    "`r`n`r`n[$(Get-Date)] Removing extension $vmssExtensionName ..."
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $vmssExtensionName | Out-Null
    Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
    Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null
    
    "`r`n`r`n[$(Get-Date)] Current extensions:"
    (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions | Format-Table
}