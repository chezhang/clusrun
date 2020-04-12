Param(
    [string] $7z = "C:\Program Files\7-Zip\7z.exe"
)

$version = go version
if ($version) {
    if (!(Test-Path $7z)) {
        "$7z is not installed"
        return
    }

    cd $PSScriptRoot
    $version

    $env:GOOS="windows"
    go build ..\clus
    go build ..\clusnode
    $install = "..\deployment\install.bat"
    $uninstall = "..\deployment\uninstall.bat"
    Compress-Archive -Path clus.exe,clusnode.exe,$install,$uninstall -DestinationPath setup.zip -Force
    rm clus.exe,clusnode.exe
    
    $env:GOOS="linux"
    go build ..\clus
    go build ..\clusnode
    $install = "..\deployment\install.sh"
    $uninstall = "..\deployment\uninstall.sh"
    rm setup.tar,setup.tar.gz -ErrorAction SilentlyContinue
    &$7z a -ttar setup.tar clus
    &$7z a -ttar setup.tar clusnode
    &$7z a -ttar setup.tar $install
    &$7z a -ttar setup.tar $uninstall
    &$7z a -tgzip setup.tar.gz setup.tar
    rm clus,clusnode,setup.tar

    cp ..\deployment\setup.ps1 .
    cp ..\deployment\setup.sh .
    cp ..\deployment\vmss.ps1 .
} else {
    "Golang is not installed"
}