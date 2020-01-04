Param(
    [Parameter(Mandatory = $true)]
    [string] $resourceGroup, 
    [Parameter(Mandatory = $true)]
    [string] $vmssName,
    [Parameter(Mandatory = $true)]
    [string] $headnodes
)

$vmssExtensionName = "Install_clusrun"
$reinstallParameter = ""

$vmss = Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -ErrorAction Stop
$extensions = (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions

"Current extensions:"
$extensions

$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
if($clusrun) {
    $reinstallParameter = "-reinstall"
    $clusrun.StatusesSummary | Format-Table
    
    "Uninstalling extension $vmssExtensionName ..."
    Remove-AzVmssExtension -VirtualMachineScaleSet $vmss -Name $vmssExtensionName | Out-Null
    Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
    Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null
    
    "Current extensions:"
    (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions | Format-Table
}

"Installing extension $vmssExtensionName ..."

$installClusrun = @{
  "fileUris" = (,"https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.ps1");
  "commandToExecute" = "powershell -ExecutionPolicy Unrestricted -File setup.ps1 $reinstallParameter -headnodes `"$headnodes`" >`"%cd%\clusrun.setup.log`" 2>&1"
}

$vmss = Add-AzVmssExtension `
  -VirtualMachineScaleSet $vmss `
  -Name "$vmssExtensionName" `
  -Publisher "Microsoft.Compute" `
  -Type "CustomScriptExtension" `
  -TypeHandlerVersion 1.9 `
  -Setting $installClusrun
Update-AzVmss -ResourceGroupName $resourceGroup -Name $vmssName -VirtualMachineScaleSet $vmss 2>&1 | Out-Null
Update-AzVmssInstance -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceId "*" 2>&1 | Out-Null

"Current extensions:"
$extensions = (Get-AzVmss -ResourceGroupName $resourceGroup -VMScaleSetName $vmssName -InstanceView).Extensions
$extensions | Format-Table
$clusrun = $extensions | Where-Object {$_.Name -eq $vmssExtensionName} 
$clusrun.StatusesSummary | Format-Table
