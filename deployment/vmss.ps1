Param(
    [Parameter(Mandatory = $true)]
    [string] $resourceGroup, 
    [Parameter(Mandatory = $true)]
    [string] $vmssName,
    [string] $headnodes,
    [switch] $uninstall = $false
)

$baseUrl = "https://github.com/chezhang/clusrun/releases/download/v0.2.latest"
$vmssExtensionName = "Install_clusrun"
$installParameter = ""

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

"[$(Get-Date)] Current extensions:"
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
    if (!$clusrun) {
        "No clusrun to uninstall"
        return
    }
    $vmssExtensionName = "Uninstall_clusrun"
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
            "commandToExecute" = "powershell -ExecutionPolicy Unrestricted -File setup.ps1 $installParameter -headnodes `"$headnodes`" -setup_url setup.zip >`"%cd%\clusrun.setup.log`" 2>&1"
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
            "commandToExecute" = "bash setup.sh $installParameter -h `"$headnodes`" -s setup.tar.gz"
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