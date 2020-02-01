Param(
    [Parameter(Mandatory = $true)]
    [string] $resourceGroup, 
    [Parameter(Mandatory = $true)]
    [string] $vmssName,
    [Parameter(Mandatory = $false)]
    [string] $headnodes
)

$vmssExtensionName = "Install_clusrun"
$reinstallParameter = ""
$baseUrl = "https://github.com/chezhang/clusrun/releases/download/0.1.0"

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

if (!$headnodes) {
    $firstInstance = Get-AzVmssVM -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId 1
    if (!$firstInstance) {
        "Can not get the first instance in VMSS"
        return
    }
    $name = $firstInstance.Name
    $headnodes = $firstInstance.OsProfile.ComputerName
    "Use the first instance $name with hostname $headnodes as headnode"
}


"Current extensions:"
$extensions

$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
if ($clusrun) {
    if ($windows) {
        $reinstallParameter = "-reinstall"
    } else {
        $reinstallParameter = "-r"
    }
    $clusrun.StatusesSummary | Format-Table
    
    "Uninstalling extension $vmssExtensionName ..."
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $vmssExtensionName | Out-Null
    Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
    Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null
    
    "Current extensions:"
    (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions | Format-Table
}

"Installing extension $vmssExtensionName ..."

if ($windows) {
    $download_file = "setup.ps1"
    $command = "powershell -ExecutionPolicy Unrestricted -File $download_file $reinstallParameter -headnodes `"$headnodes`" >`"%cd%\clusrun.setup.log`" 2>&1"
} else {
    $download_file = "setup.sh"
    $command = "bash $download_file $reinstallParameter -h `"$headnodes`""
}

$installClusrun = @{
  "fileUris" = (,"https://github.com/chezhang/clusrun/releases/download/0.1.0/$download_file");
  "commandToExecute" = $command
}

if ($windows) {
    $vmss = Add-AzVmssExtension `
        -VirtualMachineScaleSet $vmss `
        -Name "$vmssExtensionName" `
        -Publisher "Microsoft.Compute" `
        -Type "CustomScriptExtension" `
        -TypeHandlerVersion 1.9 `
        -Setting @{
            "fileUris" = (,"$baseUrl/setup.ps1");
            "commandToExecute" = "powershell -ExecutionPolicy Unrestricted -File setup.ps1 $reinstallParameter -headnodes `"$headnodes`" >`"%cd%\clusrun.setup.log`" 2>&1"
            }
} else {
    $vmss = Add-AzVmssExtension `
        -VirtualMachineScaleSet $vmss `
        -Name "$vmssExtensionName" `
        -Publisher "Microsoft.Azure.Extensions" `
        -Type "CustomScript" `
        -TypeHandlerVersion 2.1 `
        -Setting @{
            "fileUris" = (,"$baseUrl/setup.sh");
            "commandToExecute" = "bash setup.sh $reinstallParameter -h `"$headnodes`""
            }
}

Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null

"Current extensions:"
$extensions = (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions
$extensions | Format-Table
$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
$clusrun.StatusesSummary | Format-Table
