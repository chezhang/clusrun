# clusrun

A simple tool to run commands in a cluster, inspired by the [clusrun](https://docs.microsoft.com/en-us/powershell/high-performance-computing/clusrun) feature from Microsoft HPC Pack.

## Components

<img src="images/components.png" width="750">

- The worker executable `clusnode` or `clusnode.exe`

    - headnode role

        - receive heartbeats from clusnode role of workers and send node validatoin requests
        - receive node or job request from client
        - receive config request from client role of worker
        - dispatch job request to clusnode role of workers and redirect responses
        - store jobs information, nodes information and configs locally

    - clusnode role

        - send heartbeats to headnode role of workers and receive node validation requests 
        - receive job requests from headnode role of workers
        - run command locally

    - client role

        - send config request to headnode role and clusnode role of worker

- The client executable `clus` or `clus.exe`

    - send node or job request to headnode role of workers and receive response
    - display node or job information locally

## Deployment

#### Install or uninstall clusrun as service

The substance of clusrun installation is to copy the setup package to a node and to register the extracted executable as a service, the uninstallation is to unregister the service and to delete files.

- Install and uninstall clusrun on physical or virtual machine

    1. Download setup package from [Releases](https://github.com/chezhang/clusrun/releases) (`setup.zip` for Windows node, `setup.tar.gz` for Linux node)
    2. Extract it to the location you want to install clusrun
    3. Execute the install script with Administrator or root privilege to register clusrun as a service
    4. Execute the executable `clusnode` or `clusnode.exe` to set the headnode(s) for this node to report to
    5. Execute uninstall script to uninstall clusrun

    - The setup script on [Releases](https://github.com/chezhang/clusrun/releases) (`setup.ps1` for Windows node, `setup.sh` for Linux node) can help to achieve the above steps

        <details><summary>Example on Windows</summary>
    
        - Install clusrun with default parameters
        
            ```PowerShell
            PS C:\> [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            PS C:\> (New-Object System.Net.WebClient).DownloadFile("https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.ps1", "$(pwd)\setup.ps1")                                              
            PS C:\> .\setup.ps1
            04/14/2020 04:11:19  Setup clusrun: headnodes=localhost, location=C:\Program Files\clusrun, setup_url=https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.zip, reinstall=False, uninstall=False
            04/14/2020 04:11:19  Download clusrun from https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.zip
            04/14/2020 04:11:20  Extract clusrun from C:\\clusrun.setup.zip to C:\Program Files\clusrun
            04/14/2020 04:11:21  Install clusrun
            [SC] CreateService SUCCESS

            SERVICE_NAME: clusnode
                    TYPE               : 10  WIN32_OWN_PROCESS
                    STATE              : 2  START_PENDING
                                            (NOT_STOPPABLE, NOT_PAUSABLE, IGNORES_SHUTDOWN)
                    WIN32_EXIT_CODE    : 0  (0x0)
                    SERVICE_EXIT_CODE  : 0  (0x0)
                    CHECKPOINT         : 0x0
                    WAIT_HINT          : 0x7d0
                    PID                : 440
                    FLAGS              :
            symbolic link created for C:\Windows\clus.exe <<===>> C:\Program Files\clusrun\clus.exe
            symbolic link created for C:\Windows\clusnode.exe <<===>> C:\Program Files\clusrun\clusnode.exe
            04/14/2020 04:11:24  Set headnodes to localhost
            Set headnodes result:
                    "VMSS-WIN2000001:50505": Already connected
            04/14/2020 04:11:24  Clusrun is installed in C:\Program Files\clusrun
            PS C:\>
            ```

        - Executable `clus.exe` and `clusnode.exe`
        
            ```PowerShell
            PS C:\> clus.exe

            Usage:
                    clus <command> [arguments]

            The commands are:
                    node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
                    run             - run a command or script on nodes in the cluster
                    job             - list, cancel or rerun jobs in the cluster

            Usage of node:
                    clus node [options]
                    clus node -h

            Usage of run:
                    clus run [options] <command>
                    clus run -h

            Usage of job:
                    clus job [options] [jobs]
                    clus job -h

            PS C:\> clusnode.exe

            Usage:
                    clusnode <command> [options]

            The commands are:
                    start           - start the node
                    config          - configure the started node

            Usage of start:
                    clusnode start [options]
                    clusnode start -h

            Usage of config:
                    clusnode config <command> [configs]
                    clusnode config -h

            PS C:\>                                  
            ```

        - Uninstall clusrun and keep data

            ```PowerShell
            PS C:\> & 'C:\Program Files\clusrun\uninstall.bat'

            SERVICE_NAME: clusnode
                    TYPE               : 10  WIN32_OWN_PROCESS
                    STATE              : 4  RUNNING
                                            (STOPPABLE, NOT_PAUSABLE, ACCEPTS_SHUTDOWN)
                    WIN32_EXIT_CODE    : 0  (0x0)
                    SERVICE_EXIT_CODE  : 0  (0x0)
                    CHECKPOINT         : 0x0
                    WAIT_HINT          : 0x0
            [SC] DeleteService SUCCESS
            PS C:\> dir 'C:\Program Files\clusrun\'


                Directory: C:\Program Files\clusrun


            Mode                LastWriteTime         Length Name
            ----                -------------         ------ ----
            d-----        4/14/2020   4:11 AM                clusnode.exe.db
            d-----        4/14/2020   4:11 AM                clusnode.exe.logs
            -a----        4/14/2020   4:11 AM            411 clusnode.exe.config
            -a----        4/14/2020  11:33 AM            406 uninstall.bat


            PS C:\>
            ```

        - Uninstall clusrun and cleanup data

            ```PowerShell
            PS C:\> & 'C:\Program Files\clusrun\uninstall.bat' -cleanup
            [SC] OpenService FAILED 1060:

            The specified service does not exist as an installed service.

            [SC] OpenService FAILED 1060:

            The specified service does not exist as an installed service.

            Could Not Find C:\Program Files\clusrun\clusnode.exe
            Could Not Find C:\Program Files\clusrun\clus.exe
            Could Not Find C:\Windows\clusnode.exe
            Could Not Find C:\Windows\clus.exe
            The batch file cannot be found.
            The batch file cannot be found.
            PS C:\> dir 'C:\Program Files\clusrun\'
            PS C:\>
            ```

        </details>

        <details><summary>Example on Linux</summary>
    
        - Install clusrun with default parameters

            ```Bash
            root@ubuntu1804:~# wget https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.sh
            --2020-04-14 04:21:44--  https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.sh
            Resolving github.com (github.com)... 13.237.44.5
            Connecting to github.com (github.com)|13.237.44.5|:443... connected.
            HTTP request sent, awaiting response... 302 Found
            Location: https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/d9cba080-7e47-11ea-9278-49c4fd988216?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200414%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200414T042144Z&X-Amz-Expires=300&X-Amz-Signature=8a57e2c8d7c43c9d82389bae109cea50b3f1e49eb8a510119ff99a5ded01416f&X-Amz-SignedHeaders=host&actor_id=0&repo_id=230954016&response-content-disposition=attachment%3B%20filename%3Dsetup.sh&response-content-type=application%2Foctet-stream [following]
            --2020-04-14 04:21:45--  https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/d9cba080-7e47-11ea-9278-49c4fd988216?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200414%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200414T042144Z&X-Amz-Expires=300&X-Amz-Signature=8a57e2c8d7c43c9d82389bae109cea50b3f1e49eb8a510119ff99a5ded01416f&X-Amz-SignedHeaders=host&actor_id=0&repo_id=230954016&response-content-disposition=attachment%3B%20filename%3Dsetup.sh&response-content-type=application%2Foctet-stream
            Resolving github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)... 52.216.248.196
            Connecting to github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)|52.216.248.196|:443... connected.
            HTTP request sent, awaiting response... 200 OK
            Length: 1213 (1.2K) [application/octet-stream]
            Saving to: ‘setup.sh’

            setup.sh               100%[===========================>]   1.18K  --.-KB/s    in 0s

            2020-04-14 04:21:45 (40.6 MB/s) - ‘setup.sh’ saved [1213/1213]

            root@ubuntu1804:~# chmod +x setup.sh
            root@ubuntu1804:~# ./setup.sh
            --2020-04-14 04:21:56--  https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.tar.gz
            Resolving github.com (github.com)... 13.237.44.5
            Connecting to github.com (github.com)|13.237.44.5|:443... connected.
            HTTP request sent, awaiting response... 302 Found
            Location: https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/da643700-7e47-11ea-96f4-2f2796fb36f4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200414%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200414T042157Z&X-Amz-Expires=300&X-Amz-Signature=0bc7676267667b8d5d7d9b5b1773bdafefa254bbd9f98640bed5cffd2c2f4750&X-Amz-SignedHeaders=host&actor_id=0&repo_id=230954016&response-content-disposition=attachment%3B%20filename%3Dsetup.tar.gz&response-content-type=application%2Foctet-stream [following]
            --2020-04-14 04:21:57--  https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/da643700-7e47-11ea-96f4-2f2796fb36f4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200414%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200414T042157Z&X-Amz-Expires=300&X-Amz-Signature=0bc7676267667b8d5d7d9b5b1773bdafefa254bbd9f98640bed5cffd2c2f4750&X-Amz-SignedHeaders=host&actor_id=0&repo_id=230954016&response-content-disposition=attachment%3B%20filename%3Dsetup.tar.gz&response-content-type=application%2Foctet-stream
            Resolving github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)... 52.216.27.180
            Connecting to github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)|52.216.27.180|:443... connected.
            HTTP request sent, awaiting response... 200 OK
            Length: 13395728 (13M) [application/octet-stream]
            Saving to: ‘clusrun.setup.tar.gz’

            clusrun.setup.tar.gz   100%[===========================>]  12.77M  4.29MB/s    in 3.0s

            2020-04-14 04:22:01 (4.29 MB/s) - ‘clusrun.setup.tar.gz’ saved [13395728/13395728]

            clus
            clusnode
            install.sh
            uninstall.sh
            /usr/local/clusrun /usr/local/clusrun
            /usr/local/clusrun
            Set headnodes result:
                    "UBUNTU1804:50505": Already connected

            Clusrun is installed in /usr/local/clusrun

            root@ubuntu1804:~#
            ```

        - Executable `clus` and `clusnode`

            ```Bash
            root@ubuntu1804:~# clus

            Usage:
                    clus <command> [arguments]

            The commands are:
                    node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
                    run             - run a command or script on nodes in the cluster
                    job             - list, cancel or rerun jobs in the cluster

            Usage of node:
                    clus node [options]
                    clus node -h

            Usage of run:
                    clus run [options] <command>
                    clus run -h

            Usage of job:
                    clus job [options] [jobs]
                    clus job -h

            root@ubuntu1804:~# clusnode

            Usage:
                    clusnode <command> [options]

            The commands are:
                    start           - start the node
                    config          - configure the started node

            Usage of start:
                    clusnode start [options]
                    clusnode start -h

            Usage of config:
                    clusnode config <command> [configs]
                    clusnode config -h

            root@ubuntu1804:~#
            ```

        - Uninstall clusrun and keep data

            ```Bash
            root@ubuntu1804:~# /usr/local/clusrun/uninstall.sh
            root@ubuntu1804:~# ll /usr/local/clusrun/
            total 24
            drwxr-xr-x  4 root root 4096 Apr 14 04:23 ./
            drwxr-xr-x 11 root root 4096 Apr 12 12:56 ../
            -rw-r--r--  1 root root  401 Apr 14 04:22 clusnode.config
            drw-r--r--  4 root root 4096 Apr 14 04:22 clusnode.db/
            drw-r--r--  2 root root 4096 Apr 14 04:22 clusnode.logs/
            -rwxrwxrwx  1 root root  386 Apr 14 03:42 uninstall.sh*
            root@ubuntu1804:~#
            ```

        - Uninstall clusrun and cleanup data

            ```Bash
            root@ubuntu1804:~# /usr/local/clusrun/uninstall.sh -cleanup
            Failed to stop clusrun.service: Unit clusrun.service not loaded.
            root@ubuntu1804:~# ll /usr/local/clusrun/
            total 8
            drwxr-xr-x  2 root root 4096 Apr 14 04:23 ./
            drwxr-xr-x 11 root root 4096 Apr 12 12:56 ../
            root@ubuntu1804:~#
            ```

        </details>

- Install, update and uninstall clusrun on Azure VMSS

    - via PowerShell

        1. Create a [VMSS](https://docs.microsoft.com/en-us/azure/virtual-machine-scale-sets/overview) on Azure
        2. Optionally, create a VM in the same subnet as the headnode
        3. Install [Azure PowerShell](https://docs.microsoft.com/en-us/powershell/azure/install-az-ps) module if it is not installed yet
        4. [Sign in](https://docs.microsoft.com/en-us/powershell/module/az.accounts/connect-azaccount) Azure account and [select](https://docs.microsoft.com/en-us/powershell/module/az.accounts/set-azcontext) corresponding subscription in PowerShell
        5. Download and run [vmss.ps1](https://github.com/chezhang/clusrun/releases/download/v0.2.0/vmss.ps1) to install clusrun as a VMSS extension. Specify the node names of headnodes, to which the clusrun client should connect, when running the script, or the first instance in the VMSS will be used as a headnode

            <details><summary>Example</summary>

            - Install clusrun with default headnode

                ```PowerShell
                PS D:\> [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
                PS D:\> (New-Object System.Net.WebClient).DownloadFile("https://github.com/chezhang/clusrun/releases/download/v0.2.0/vmss.ps1", "$(pwd)\vmss.ps1")
                PS D:\> \vmss.ps1 -resourceGroup vmss-win -vmssName vmss-win2019
                Use the first instance vmss-win2019_1 with hostname vmss-win2000001 as headnode
                [04/14/2020 10:43:43] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount, Micr…


                [04/14/2020 10:43:43] Adding extension Install_clusrun ...


                [04/14/2020 10:47:01] Current extensions:


                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}
                Install_clusrun                         {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


                Code                        Count
                ----                        -----
                ProvisioningState/succeeded    40

                PS D:\>
                ```

            - Update/Reinstall clusrun with default headnode

                ```Powershell
                PS D:\> \vmss.ps1 -resourceGroup vmss-win -vmssName vmss-win2019
                Use the first instance vmss-win2019_1 with hostname vmss-win2000001 as headnode
                [04/14/2020 10:59:04] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}
                Install_clusrun                         {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


                Code                        Count
                ----                        -----
                ProvisioningState/succeeded    40



                [04/14/2020 10:59:04] Removing extension Install_clusrun ...


                [04/14/2020 11:01:58] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}



                [04/14/2020 11:02:00] Adding extension Install_clusrun ...


                [04/14/2020 11:04:21] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}
                Install_clusrun                         {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


                Code                        Count
                ----                        -----
                ProvisioningState/succeeded    40

                PS D:\>
                ```

            - Uninstall clusrun

                ```Powershell
                PS D:\> \vmss.ps1 -resourceGroup vmss-win -vmssName vmss-win2019 -uninstall
                Use the first instance vmss-win2019_1 with hostname vmss-win2000001 as headnode
                [04/14/2020 11:34:48] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}
                Install_clusrun                         {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


                Code                        Count
                ----                        -----
                ProvisioningState/succeeded    40



                [04/14/2020 11:34:48] Removing extension Install_clusrun ...


                [04/14/2020 11:37:13] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}



                [04/14/2020 11:37:17] Adding extension Uninstall_clusrun ...


                [04/14/2020 11:39:41] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}
                Uninstall_clusrun                       {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


                Code                        Count
                ----                        -----
                ProvisioningState/succeeded    40



                [04/14/2020 11:39:43] Removing extension Uninstall_clusrun ...


                [04/14/2020 11:42:05] Current extensions:

                Name                                    StatusesSummary
                ----                                    ---------------
                Microsoft.Azure.Geneva.GenevaMonitoring {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}

                PS D:\>
                ```

            </details>

    - via Azure portal

        1. Create a VMSS on Azure
        2. Optionally, create a VM in the same subnet as the headnode
        3. Open `Extensions` page in `Settings` of the VMSS and click `Add`
        4. Select `Custom Script Extension` and click `Create`
        5. Download setup script from [Releases](https://github.com/chezhang/clusrun/releases) (`setup.ps1` for Windows, `setup.sh` for Linux)
        6. Click `Browse` in `Install extension` page, select or create a storage account and upload the setup script to a container in it, then select the setup script as the `Script file`
        7. Set `Arguments` to specify the headnode(s) (`-headnode(s) "<headnode(s)>"` for Windows, `-h "<headnodes>"` for Linux)
        8. Click `OK` to install clusrun to the VMSS as extension
        9. Upgrade VMSS instances to the latest model to apply the extension change
        10. Add `Custom Script Extension` again and set `Arguments` to update or uninstall clusrun (`-reinstall` or `-uninstall` for Windows, `-r` or `-u` for Linux)

#### Start clusrun manually

- Start clusrun on physical or virtual machine

    1. Download setup package from [Releases](https://github.com/chezhang/clusrun/releases) (`setup.zip` for Windows node, `setup.tar.gz` for Linux node)
    2. Extract it to the location you want to install clusrun
    3. Start clusrun with extracted executable `clusnode` or `clusnode.exe`

- Start clusrun inside docker container

    Docker container with clusrun can be started as a node on physical or virtual machine to join a cluster.

    - Docker container node on Linux

        1. Install docker if it is not installed
        2. Download clusrun setup package `setup.tar.gz` from [Releases](https://github.com/chezhang/clusrun/releases) and extract the executable `clusnode`
        3. Start docker container(s) with clusrun being hosted on available port(s) of host

        <details><summary>Example</summary>

        ```Bash
        root@ubuntu:~# docker -v
        Docker version 18.09.7, build 2d0083d
        root@ubuntu:~# wget -q https://github.com/chezhang/clusrun/releases/download/0.2.0/setup.tar.gz
        root@ubuntu:~# tar zxvf setup.tar.gz
        clus
        clusnode
        install.sh
        uninstall.sh
        root@ubuntu:~# headnode=$(hostname):60000
        root@ubuntu:~# for port in {60000..60010}; do docker run -v $(pwd):/tmp:ro -p $port:$port -d ubuntu bash -c "cp /tmp/clusnode . && ./clusnode start -headnodes $headnode -host $(hostname):$port"; done
        638f1f5703d9d20d0d1662907891e59970fbe138e7b8fdcc996ad5d89c4a4ccb
        02658001db5ec0e504ff57e45135d69898e747c702f0aac589f6f21fceb3545d
        d925c729b2eb77a338571aabd5140f6248bd34b662c4926b9a2cd4dad14de9f0
        64fa2ce25f18cd64184ca904293497444367c44c3bcdf256a4fc3d477a662b15
        0e626052110baba81b0fe7b0a856fc9616640c0f8396789669744282ac4c33b9
        77eab7045773bef6c7917f1a38cc14b6a2d1735794a02bfff00bfc1b3450f096
        b95d4600578f1b319f2506623743e8ce316819a0c938d825f285fb95f8c620ef
        80f2fc1fd219adc8bb2f5ab5d5f19e64f0a2a01822b02fd1f06c099d7a57b82a
        44a286831dc88e687c08f13967c007ae77cfa4567772ccf84bac46d1fc15dcc8
        6ce13f870ff51d3aa69f34c175c8be67b80d695cf8d93657840f0cb6b33cf834
        eadf99a431416a3ccb4996c211bd5eb553ead4d37b37636acd5ceab83797e771
        root@ubuntu:~# ./clus node -headnode $headnode
        Node                         State
        --------------------------   -----
        02658001DB5E(UBUNTU:60001)   Ready
        0E626052110B(UBUNTU:60004)   Ready
        44A286831DC8(UBUNTU:60008)   Ready
        638F1F5703D9(UBUNTU:60000)   Ready
        64FA2CE25F18(UBUNTU:60003)   Ready
        6CE13F870FF5(UBUNTU:60009)   Ready
        77EAB7045773(UBUNTU:60005)   Ready
        80F2FC1FD219(UBUNTU:60007)   Ready
        B95D4600578F(UBUNTU:60006)   Ready
        D925C729B2EB(UBUNTU:60002)   Ready
        EADF99A43141(UBUNTU:60010)   Ready
        ----------------------------------
        Node count: 11
        root@ubuntu:~#
        ```
        </details>

    - Docker container node on Windows is not supported currently

## Usage examples

### Client executable `clus` or `clus.exe`

- Show usage

    ```CMD or Bash
    clus
    ```
    
    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
            run             - run a command or script on nodes in the cluster
            job             - list, cancel or rerun jobs in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
            run             - run a command or script on nodes in the cluster
            job             - list, cancel or rerun jobs in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h

    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus -h

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
            run             - run a command or script on nodes in the cluster
            job             - list, cancel or rerun jobs in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus -h

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list nodes, add nodes to groups or remove nodes from groups in the cluster
            run             - run a command or script on nodes in the cluster
            job             - list, cancel or rerun jobs in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h

    root@vmss-ubun000000:~#
    ```
    </details>

<details><summary>clus node</summary>

#### Manage nodes in cluster by command `clus node`

- Show usage

    ```CMD or Bash
    clus node -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -h
    Usage of clus node options:
    -addgroups string
            add nodes to the specified node groups
    -format string
            format the nodes in table, list or group (default "table")
    -groupby string
            group the nodes by state or node group
    -groups string
            filter nodes in the specified node groups
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to filter nodes in intersection (union if not specified) of node groups
    -orderby string
            sort the nodes by node name or node groups (default "name")
    -pattern string
            filter nodes matching the specified regular expression pattern
    -removegroups string
            remove nodes from the specified node groups
    -state string
            filter nodes in the specified state (ready, error or lost)

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -h
    Usage of clus node options:
    -addgroups string
            add nodes to the specified node groups
    -format string
            format the nodes in table, list or group (default "table")
    -groupby string
            group the nodes by state or node group
    -groups string
            filter nodes in the specified node groups
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to filter nodes in intersection (union if not specified) of node groups
    -orderby string
            sort the nodes by node name or node groups (default "name")
    -pattern string
            filter nodes matching the specified regular expression pattern
    -removegroups string
            remove nodes from the specified node groups
    -state string
            filter nodes in the specified state (ready, error or lost)
    root@vmss-ubun000000:~#
    ```
    </details>

- List all nodes in the cluster

    ```CMD or Bash
    clus node
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~#
    ```
    </details>

- List nodes with filters

    ```CMD or Bash
    clus node -groups <node groups> [-intersec]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -groups windows,vm
    Node              State   Groups
    ---------------   -----   -----------
    VMSS-UBUN000000   Ready   linux, vm
    WIN2016           Ready   vm, windows
    -------------------------------------
    Node count: 2

    c:\>clus node -groups windows,vm -intersect
    Node      State   Groups
    -------   -----   -----------
    WIN2016   Ready   vm, windows
    -----------------------------
    Node count: 1

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -groups linux,container
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    --------------------------------------------------------------
    Node count: 2
    root@vmss-ubun000000:~# clus node -groups linux,container -intersect
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    --------------------------------------------------------------
    Node count: 1
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus node -pattern <regular expression>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -pattern VMSS-UBUN000000
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    --------------------------------------------------------------
    Node count: 2

    c:\>clus node -pattern ^VMSS-UBUN000000$
    Node              State   Groups
    ---------------   -----   ---------
    VMSS-UBUN000000   Ready   linux, vm
    -----------------------------------
    Node count: 1

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -pattern VMSS-UBUN000000
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    --------------------------------------------------------------
    Node count: 2
    root@vmss-ubun000000:~# clus node -pattern ^VMSS-UBUN000000$
    Node              State   Groups
    ---------------   -----   ---------
    VMSS-UBUN000000   Ready   linux, vm
    -----------------------------------
    Node count: 1
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus node -state <ready/error/lost>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -state ready
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 3

    c:\>clus node -state error
    Node count: 0

    c:\>clus node -state lost
    Node                     State
    ----------------------   -----
    WIN2016(WIN2016:60000)   Lost
    ------------------------------
    Node count: 1

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -state ready
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 3
    root@vmss-ubun000000:~# clus node -state error
    Node count: 0
    root@vmss-ubun000000:~# clus node -state lost
    Node                     State
    ----------------------   -----
    WIN2016(WIN2016:60000)   Lost
    ------------------------------
    Node count: 1
    root@vmss-ubun000000:~#
    ```
    </details>

- List nodes with different displaying format

    ```CMD or Bash
    clus node -format <table/list/group>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -format table
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4

    c:\>clus node -format list
    Node   : 67A226CD8E76(VMSS-UBUN000000:60000)
    State  : Ready
    Groups : container, linux
    ----------------------------------------------------------------------------
    Node   : VMSS-UBUN000000
    State  : Ready
    Groups : linux, vm
    ----------------------------------------------------------------------------
    Node   : WIN2016
    State  : Ready
    Groups : vm, windows
    ----------------------------------------------------------------------------
    Node   : WIN2016(WIN2016:60000)
    State  : Lost
    ----------------------------------------------------------------------------
    Node count: 4

    c:\>clus node -format group -groupby nodegroup
    ----------------------------------------------------------------------------
    WIN2016(WIN2016:60000)
    --------------------------------[container]---------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)
    ----------------------------------[linux]-----------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)   VMSS-UBUN000000

    ------------------------------------[vm]------------------------------------
    VMSS-UBUN000000   WIN2016
    ---------------------------------[windows]----------------------------------
    WIN2016
    ----------------------------------------------------------------------------
    Count of nodes in no group: 1
    Count of nodes in group 'container': 1
    Count of nodes in group 'linux': 2
    Count of nodes in group 'vm': 2
    Count of nodes in group 'windows': 1

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -format table
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~# clus node -format list
    Node   : 67A226CD8E76(VMSS-UBUN000000:60000)
    State  : Ready
    Groups : container, linux
    -------------------------------------------------------------------
    Node   : VMSS-UBUN000000
    State  : Ready
    Groups : linux, vm
    -------------------------------------------------------------------
    Node   : WIN2016
    State  : Ready
    Groups : vm, windows
    -------------------------------------------------------------------
    Node   : WIN2016(WIN2016:60000)
    State  : Lost
    -------------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~# clus node -format group -groupby nodegroup
    -------------------------------------------------------------------
    WIN2016(WIN2016:60000)
    ----------------------------[container]----------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)

    ------------------------------[linux]------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)
    VMSS-UBUN000000

    -------------------------------[vm]--------------------------------
    VMSS-UBUN000000   WIN2016
    -----------------------------[windows]-----------------------------
    WIN2016
    -------------------------------------------------------------------
    Count of nodes in no group: 1
    Count of nodes in group 'container': 1
    Count of nodes in group 'linux': 2
    Count of nodes in group 'vm': 2
    Count of nodes in group 'windows': 1
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus node -groupby <state/nodegroup>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -groupby state
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost

    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4

    c:\>clus node -groupby nodegroup
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost

    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux

    VMSS-UBUN000000                       Ready   linux, vm

    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4

    c:\>clus node -groupby state -format group
    -----------------------------------[Lost]-----------------------------------
    WIN2016(WIN2016:60000)
    ----------------------------------[Ready]-----------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)   VMSS-UBUN000000
    WIN2016
    ----------------------------------------------------------------------------
    Count of nodes in state 'Lost': 1
    Count of nodes in state 'Ready': 3

    c:\>clus node -groupby nodegroup -format group
    ----------------------------------------------------------------------------
    WIN2016(WIN2016:60000)
    --------------------------------[container]---------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)
    ----------------------------------[linux]-----------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)   VMSS-UBUN000000

    ------------------------------------[vm]------------------------------------
    VMSS-UBUN000000   WIN2016
    ---------------------------------[windows]----------------------------------
    WIN2016
    ----------------------------------------------------------------------------
    Count of nodes in no group: 1
    Count of nodes in group 'container': 1
    Count of nodes in group 'linux': 2
    Count of nodes in group 'vm': 2
    Count of nodes in group 'windows': 1

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -groupby state
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost

    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~# clus node -groupby nodegroup
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost

    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux

    VMSS-UBUN000000                       Ready   linux, vm

    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~# clus node -groupby state -format group
    ------------------------------[Lost]-------------------------------
    WIN2016(WIN2016:60000)
    ------------------------------[Ready]------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)
    VMSS-UBUN000000
    WIN2016

    -------------------------------------------------------------------
    Count of nodes in state 'Lost': 1
    Count of nodes in state 'Ready': 3
    root@vmss-ubun000000:~# clus node -groupby nodegroup -format group
    -------------------------------------------------------------------
    WIN2016(WIN2016:60000)
    ----------------------------[container]----------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)

    ------------------------------[linux]------------------------------
    67A226CD8E76(VMSS-UBUN000000:60000)
    VMSS-UBUN000000

    -------------------------------[vm]--------------------------------
    VMSS-UBUN000000   WIN2016
    -----------------------------[windows]-----------------------------
    WIN2016
    -------------------------------------------------------------------
    Count of nodes in no group: 1
    Count of nodes in group 'container': 1
    Count of nodes in group 'linux': 2
    Count of nodes in group 'vm': 2
    Count of nodes in group 'windows': 1
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus node -orderby <name/nodegroups>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -orderby name
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4

    c:\>clus node -orderby nodegroup
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -orderby name
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~# clus node -orderby nodegroup
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    WIN2016(WIN2016:60000)                Lost
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~#
    ```
    </details>

- Add or remove node groups

    ```CMD or Bash
    clus node -addgroups <node groups> [-groups] [-pattern] [-state]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -addgroups windows -pattern WIN2016
    Node                     State   Groups
    ----------------------   -----   -----------
    WIN2016                  Ready   vm, windows
    WIN2016(WIN2016:60000)   Lost    windows
    --------------------------------------------
    Node count: 2
    Nodes are added to node groups: windows

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -addgroups test
    Node                                  State   Groups
    -----------------------------------   -----   ----------------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux, test
    VMSS-UBUN000000                       Ready   linux, test, vm
    WIN2016                               Ready   test, vm, windows
    WIN2016(WIN2016:60000)                Lost    test
    --------------------------------------------------------------------
    Node count: 4
    Nodes are added to node groups: test
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus node -removegroups <node groups> [-groups] [-pattern] [-state]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -removegroups windows -pattern WIN2016:60000
    Node                     State
    ----------------------   -----
    WIN2016(WIN2016:60000)   Lost
    ------------------------------
    Node count: 1
    Nodes are removed from node groups: windows

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -removegroups test
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4
    Nodes are removed from node groups: test
    root@vmss-ubun000000:~#
    ```
    </details>

- List nodes in specified cluster

    ```CMD or Bash
    clus node -headnode <node host>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus node -headnode VMSS-UBUN000000
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus node -headnode win2016
    Node                                  State   Groups
    -----------------------------------   -----   ----------------
    67A226CD8E76(VMSS-UBUN000000:60000)   Ready   container, linux
    VMSS-UBUN000000                       Ready   linux, vm
    WIN2016                               Ready   vm, windows
    WIN2016(WIN2016:60000)                Lost
    --------------------------------------------------------------
    Node count: 4
    root@vmss-ubun000000:~#
    ```
    </details>

</details>

<details><summary>clus run</summary>

#### Run command in cluster by command `clus run`

- Show usage

    ```CMD or Bash
    clus run
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run

    Usage:
    clus run [options] <command>

    Options:
    -background
            run command without printing output
    -cache int
            specify the number of characters to cache and display for output of command on each node (default 1000)
    -dump
            save the output to file
    -groups string
            specify certain node groups to run the command
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to run the command in intersection (union if not specified) of node groups
    -nodes string
            specify certain nodes to run the command
    -pattern string
            specify nodes matching a certain regular expression pattern to run the command
    -prompt int
            specify the number of nodes, the output of which will be displayed promptly (default 1)
    -script string
            specify the script file containing commands to run
    -sweep string
            perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][:step]}]"

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run

    Usage:
    clus run [options] <command>

    Options:
    -background
            run command without printing output
    -cache int
            specify the number of characters to cache and display for output of command on each node (default 1000)
    -dump
            save the output to file
    -groups string
            specify certain node groups to run the command
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to run the command in intersection (union if not specified) of node groups
    -nodes string
            specify certain nodes to run the command
    -pattern string
            specify nodes matching a certain regular expression pattern to run the command
    -prompt int
            specify the number of nodes, the output of which will be displayed promptly (default 1)
    -script string
            specify the script file containing commands to run
    -sweep string
            perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][:step]}]"
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus run -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -h
    Usage of clus run options:
    -background
            run command without printing output
    -cache int
            specify the number of characters to cache and display for output of command on each node (default 1000)
    -dump
            save the output to file
    -groups string
            specify certain node groups to run the command
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to run the command in intersection (union if not specified) of node groups
    -nodes string
            specify certain nodes to run the command
    -pattern string
            specify nodes matching a certain regular expression pattern to run the command
    -prompt int
            specify the number of nodes, the output of which will be displayed promptly (default 1)
    -script string
            specify the script file containing commands to run
    -sweep string
            perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][:step]}]"

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -h
    Usage of clus run options:
    -background
            run command without printing output
    -cache int
            specify the number of characters to cache and display for output of command on each node (default 1000)
    -dump
            save the output to file
    -groups string
            specify certain node groups to run the command
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -intersect
            specify to run the command in intersection (union if not specified) of node groups
    -nodes string
            specify certain nodes to run the command
    -pattern string
            specify nodes matching a certain regular expression pattern to run the command
    -prompt int
            specify the number of nodes, the output of which will be displayed promptly (default 1)
    -script string
            specify the script file containing commands to run
    -sweep string
            perform parametric sweep by replacing specified placeholder string in the command on each node to sequence number (in specified range and step optionally) with format "placeholder[{begin[-end][:step]}]"
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command in the cluster

    ```CMD or Bash
    clus run <command>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run hostname
    Job 1 started on 3 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 108.8428ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 109.8266ms.
    [3/3] Command finished on node WIN2016 in 112.8264ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    67a226cd8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=108.8428ms
    Max=112.8264ms
    Mean=110.4986ms
    Mid=109.8266ms
    SD=1.694295ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run hostname
    Job 72 started on 3 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 59.975ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 62.5264ms.
    [3/3] Command finished on node WIN2016 in 109.8528ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=59.975ms
    Max=109.8528ms
    Mean=77.4514ms
    Mid=62.5264ms
    SD=22.934914ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command on certain nodes

    ```CMD or Bash
    clus run -groups <node groups> [-intersect]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -groups vm hostname
    Job 2 started on 2 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [VMSS-UBUN000000]: vmss-ubun000000
    [1/2] Command finished on node VMSS-UBUN000000 in 112.6209ms.
    [2/2] Command finished on node WIN2016 in 114.6214ms.

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=112.6209ms
    Max=114.6214ms
    Mean=113.62115ms
    Mid=113.62115ms
    SD=1.00025ms

    2 of 2 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -groups vm hostname
    Job 73 started on 2 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [VMSS-UBUN000000]: vmss-ubun000000
    [1/2] Command finished on node VMSS-UBUN000000 in 51.0999ms.
    [2/2] Command finished on node WIN2016 in 94.0386ms.

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=51.0999ms
    Max=94.0386ms
    Mean=72.56925ms
    Mid=72.56925ms
    SD=21.46935ms

    2 of 2 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus run -pattern <regular expression>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -pattern WIN2016 hostname
    Job 3 started on 1 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [WIN2016]: win2016
    [1/1] Command finished on node WIN2016 in 70.9979ms.

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime: Min=70.9979ms, Max=70.9979ms, Mean=70.9979ms, Mid=70.9979ms, SD=0s
    1 of 1 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -pattern VMSS hostname
    Job 74 started on 2 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/2] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 54.392ms.
    [2/2] Command finished on node VMSS-UBUN000000 in 63.4899ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------------------------------------------------
    Runtime:
    Min=54.392ms
    Max=63.4899ms
    Mean=58.94095ms
    Mid=58.94095ms
    SD=4.54895ms

    2 of 2 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clus run -nodes <node names>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -nodes WIN2016,VMSS-UBUN000000 hostname
    Job 5 started on 2 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [VMSS-UBUN000000]: vmss-ubun000000
    [1/2] Command finished on node VMSS-UBUN000000 in 56.158ms.
    [2/2] Command finished on node WIN2016 in 69.1561ms.

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=56.158ms
    Max=69.1561ms
    Mean=62.65705ms
    Mid=62.65705ms
    SD=6.49905ms

    2 of 2 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -nodes VMSS-UBUN000000,WIN2016 hostname   Job 75 started on 2 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [VMSS-UBUN000000]: vmss-ubun000000
    [1/2] Command finished on node VMSS-UBUN000000 in 51.4004ms.
    [2/2] Command finished on node WIN2016 in 94.261ms.

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=51.4004ms
    Max=94.261ms
    Mean=72.8307ms
    Mid=72.8307ms
    SD=21.4303ms

    2 of 2 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command in background

    ```CMD or Bash
    clus run -backgroud
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -background hostname
    Job 6 started on 3 nodes in cluster localhost:50505.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -background hostname
    Job 76 started on 3 nodes in cluster localhost:50505.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command with specified cache size

    ```CMD or Bash
    clus run -cache <character number>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -cache 5 hostname
    Job 7 started on 3 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [1/3] Command finished on node VMSS-UBUN000000 in 57.4392ms.
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 60.4381ms.
    [3/3] Command finished on node WIN2016 in 73.4352ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    (Truncated)
    ...8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    (Truncated)
    ...0000

    ---------------------------------[WIN2016]----------------------------------
    (Truncated)
    ...016

    ----------------------------------------------------------------------------
    Runtime:
    Min=57.4392ms
    Max=73.4352ms
    Mean=63.770833ms
    Mid=60.4381ms
    SD=6.942542ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -cache 5 hostname
    Job 77 started on 3 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 53.1031ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 58.7311ms.
    [3/3] Command finished on node WIN2016 in 88.8155ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    (Truncated)
    ...8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    (Truncated)
    ...0000

    --------------------------------[WIN2016]---------------------------------
    (Truncated)
    ...016

    --------------------------------------------------------------------------
    Runtime:
    Min=53.1031ms
    Max=88.8155ms
    Mean=66.883233ms
    Mid=58.7311ms
    SD=15.67773ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command displaying output of different number of nodes promptly

    ```CMD or Bash
    clus run -prompt <node number>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -prompt 10 hostname
    Job 8 started on 3 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [VMSS-UBUN000000]: vmss-ubun000000
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node VMSS-UBUN000000 in 28.0018ms.
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 30.0007ms.
    [WIN2016]: win2016
    [3/3] Command finished on node WIN2016 in 64.2562ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    67a226cd8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=28.0018ms
    Max=64.2562ms
    Mean=40.7529ms
    Mid=30.0007ms
    SD=16.639365ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -prompt 10 hostname
    Job 78 started on 3 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 54.4323ms.
    [VMSS-UBUN000000]: vmss-ubun000000
    [2/3] Command finished on node VMSS-UBUN000000 in 61.0101ms.
    [WIN2016]: win2016
    [3/3] Command finished on node WIN2016 in 98.7498ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=54.4323ms
    Max=98.7498ms
    Mean=71.3974ms
    Mid=61.0101ms
    SD=19.5266ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command and dump output

    ```CMD or Bash
    clus run -dump
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -dump hostname
    Job 9 started on 3 nodes in cluster localhost:50505.
    Dumping output to c:\clus.run.20200415174918.800091500

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [1/3] Command finished on node VMSS-UBUN000000 in 31.0957ms.
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 64.5129ms.
    [3/3] Command finished on node WIN2016 in 83.4806ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    67a226cd8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=31.0957ms
    Max=83.4806ms
    Mean=59.6964ms
    Mid=64.5129ms
    SD=21.655537ms

    3 of 3 nodes succeeded.
    Output is dumped to c:\clus.run.20200415174918.800091500

    c:\>dir clus.run.20200415174918.800091500
    Volume in drive C is Windows
    Volume Serial Number is 56D1-9C35

    Directory of c:\clus.run.20200415174918.800091500

    04/15/2020  05:49 PM    <DIR>          .
    04/15/2020  05:49 PM    <DIR>          ..
    04/15/2020  05:49 PM                 0 67A226CD8E76(VMSS-UBUN000000.60000).err
    04/15/2020  05:49 PM                13 67A226CD8E76(VMSS-UBUN000000.60000).out
    04/15/2020  05:49 PM                 0 VMSS-UBUN000000.err
    04/15/2020  05:49 PM                16 VMSS-UBUN000000.out
    04/15/2020  05:49 PM                 0 WIN2016.err
    04/15/2020  05:49 PM                 9 WIN2016.out
                6 File(s)             38 bytes
                2 Dir(s)  124,614,762,496 bytes free

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -dump hostname
    Job 79 started on 3 nodes in cluster localhost:50505.
    Dumping output to /root/clus.run.20200415174944.716807900

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 47.7575ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 53.568ms.
    [3/3] Command finished on node WIN2016 in 90.1797ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=47.7575ms
    Max=90.1797ms
    Mean=63.835066ms
    Mid=53.568ms
    SD=18.778893ms

    3 of 3 nodes succeeded.
    Output is dumped to /root/clus.run.20200415174944.716807900
    root@vmss-ubun000000:~# ll clus.run.20200415174944.716807900/
    total 20
    drw-r--r-- 2 root root 4096 Apr 15 17:49  ./
    drwx------ 8 root root 4096 Apr 15 17:49  ../
    -rw-r--r-- 1 root root    0 Apr 15 17:49 '67A226CD8E76(VMSS-UBUN000000.60000).err'
    -rw-r--r-- 1 root root   13 Apr 15 17:49 '67A226CD8E76(VMSS-UBUN000000.60000).out'
    -rw-r--r-- 1 root root    0 Apr 15 17:49  VMSS-UBUN000000.err
    -rw-r--r-- 1 root root   16 Apr 15 17:49  VMSS-UBUN000000.out
    -rw-r--r-- 1 root root    0 Apr 15 17:49  WIN2016.err
    -rw-r--r-- 1 root root    9 Apr 15 17:49  WIN2016.out
    root@vmss-ubun000000:~#
    ```
    </details>

- Run commands in a script

    ```CMD or Bash
    clus run -script <file>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>notepad script.txt

    c:\>more script.txt
    hostname
    echo hostname
    hostname

    c:\>clus run -script script.txt
    Job 10 started on 3 nodes in cluster localhost:50505.

    ----------------------------------Command-----------------------------------
    hostname
    echo hostname
    hostname
    ----------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: /clusnode.db/VMSS-UBUN000000.60000.command/WIN2016.50505.10.sh: line 1: $'hostname\r': command not found
    [67A226CD8E76(VMSS-UBUN000000:60000)]: hostname
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 28.9948ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 31.0032ms.
    [3/3] Command finished on node WIN2016 in 81.9474ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    /clusnode.db/VMSS-UBUN000000.60000.command/WIN2016.50505.10.sh: line 1: $'hostname\r': command not found
    hostname
    67a226cd8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    /usr/local/clusrun/clusnode.db/VMSS-UBUN000000.50505.command/WIN2016.50505.10.sh: line 1: $'hostname\r': command not found
    hostname
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016
    hostname
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=28.9948ms
    Max=81.9474ms
    Mean=47.315133ms
    Mid=31.0032ms
    SD=24.502433ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# vim script
    root@vmss-ubun000000:~# cat script
    hostname
    echo 1
    hostname
    root@vmss-ubun000000:~# clus run -script script
    Job 80 started on 3 nodes in cluster localhost:50505.

    ---------------------------------Command----------------------------------
    hostname
    echo 1
    hostname

    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 1
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 41.9381ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 48.7202ms.
    [3/3] Command finished on node WIN2016 in 99.1658ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76
    1
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000
    1
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016
    1
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=41.9381ms
    Max=99.1658ms
    Mean=63.2747ms
    Mid=48.7202ms
    SD=25.529427ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

- Run command with different content on each node

    ```CMD or Bash
    clus run -sweep <placeholder[{begin[-end][:step]}]>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -sweep ?? echo ??
    Job 12 started on 3 nodes in cluster localhost:50505.

    Sweep parameter: ??
    ----------------------------------Command-----------------------------------
    echo ??
    ----------------------------------------------------------------------------

    [1/3] Command finished on node VMSS-UBUN000000 in 53.9991ms.
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 0
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 80.0054ms.
    [3/3] Command finished on node WIN2016 in 81.0067ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    0

    -----------------------------[VMSS-UBUN000000]------------------------------
    1

    ---------------------------------[WIN2016]----------------------------------
    2

    ----------------------------------------------------------------------------
    Runtime:
    Min=53.9991ms
    Max=81.0067ms
    Mean=71.6704ms
    Mid=80.0054ms
    SD=12.50218ms

    3 of 3 nodes succeeded.

    c:\>clus run -sweep ??{10-9:-1} echo ??
    Job 13 started on 3 nodes in cluster localhost:50505.

    Sweep parameter: ??{10-9:-1}
    ----------------------------------Command-----------------------------------
    echo ??
    ----------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 10
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 24.0771ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 25.9966ms.
    [3/3] Command finished on node WIN2016 in 51.1295ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    10

    -----------------------------[VMSS-UBUN000000]------------------------------
    9

    ---------------------------------[WIN2016]----------------------------------
    10

    ----------------------------------------------------------------------------
    Runtime:
    Min=24.0771ms
    Max=51.1295ms
    Mean=33.7344ms
    Mid=25.9966ms
    SD=12.32513ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -sweep ?? echo ??
    Job 82 started on 3 nodes in cluster localhost:50505.

    Sweep parameter: ??
    ---------------------------------Command----------------------------------
    echo ??
    --------------------------------------------------------------------------

    [1/3] Command finished on node VMSS-UBUN000000 in 47.2975ms.
    [67A226CD8E76(VMSS-UBUN000000:60000)]: 0
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 47.6821ms.
    [3/3] Command finished on node WIN2016 in 91.2192ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    0

    ----------------------------[VMSS-UBUN000000]-----------------------------
    1

    --------------------------------[WIN2016]---------------------------------
    2

    --------------------------------------------------------------------------
    Runtime:
    Min=47.2975ms
    Max=91.2192ms
    Mean=62.066266ms
    Mid=47.6821ms
    SD=20.614834ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~# clus run -sweep ??{10-9:-1} echo ??
    Job 83 started on 3 nodes in cluster localhost:50505.

    Sweep parameter: ??{10-9:-1}
    ---------------------------------Command----------------------------------
    echo ??
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 10
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 60.5268ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 73.7394ms.
    [3/3] Command finished on node WIN2016 in 108.5368ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    10

    ----------------------------[VMSS-UBUN000000]-----------------------------
    9

    --------------------------------[WIN2016]---------------------------------
    10

    --------------------------------------------------------------------------
    Runtime:
    Min=60.5268ms
    Max=108.5368ms
    Mean=80.934333ms
    Mid=73.7394ms
    SD=20.249532ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>
    

- Run command in specified cluster

    ```CMD or Bash
    clus run -headnode <node host>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -headnode VMSS-UBUN000000 hostname
    Job 81 started on 3 nodes in cluster VMSS-UBUN000000:50505.

    ----------------------------------Command-----------------------------------
    hostname
    ----------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 51.5474ms.
    [2/3] Command finished on node VMSS-UBUN000000 in 54.548ms.
    [3/3] Command finished on node WIN2016 in 103.8009ms.

    -------------------[67A226CD8E76(VMSS-UBUN000000:60000)]--------------------
    67a226cd8e76

    -----------------------------[VMSS-UBUN000000]------------------------------
    vmss-ubun000000

    ---------------------------------[WIN2016]----------------------------------
    win2016

    ----------------------------------------------------------------------------
    Runtime:
    Min=51.5474ms
    Max=103.8009ms
    Mean=69.965433ms
    Mid=54.548ms
    SD=23.956627ms

    3 of 3 nodes succeeded.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -headnode win2016 hostname
    Job 11 started on 3 nodes in cluster win2016:50505.

    ---------------------------------Command----------------------------------
    hostname
    --------------------------------------------------------------------------

    [67A226CD8E76(VMSS-UBUN000000:60000)]: 67a226cd8e76
    [1/3] Command finished on node VMSS-UBUN000000 in 52.6636ms.
    [2/3] Command finished on node 67A226CD8E76(VMSS-UBUN000000:60000) in 52.8099ms.
    [3/3] Command finished on node WIN2016 in 61.2312ms.

    ------------------[67A226CD8E76(VMSS-UBUN000000:60000)]-------------------
    67a226cd8e76

    ----------------------------[VMSS-UBUN000000]-----------------------------
    vmss-ubun000000

    --------------------------------[WIN2016]---------------------------------
    win2016

    --------------------------------------------------------------------------
    Runtime:
    Min=52.6636ms
    Max=61.2312ms
    Mean=55.568233ms
    Mid=52.8099ms
    SD=4.004767ms

    3 of 3 nodes succeeded.
    root@vmss-ubun000000:~#
    ```
    </details>

</details>

<details><summary>clus job</summary>

#### Manage jobs in cluster by command `clus job`

- Show usage

    ```CMD or Bash
    clus job -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job -h
    Usage of clus job options:
    -cancel
            cancel jobs
    -format string
            format the jobs in table or list
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -rerun
            rerun jobs

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job -h
    Usage of clus job options:
    -cancel
            cancel jobs
    -format string
            format the jobs in table or list
    -headnode string
            specify the headnode to connect (default "localhost:50505")
    -rerun
            rerun jobs
    root@vmss-ubun000000:~#
    ```
    </details>

- List all jobs in the cluster

    ```CMD or Bash
    clus job
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job
    Id   State      Nodes   Command
    --   --------   -----   -------------------------------------
    1    Finished   3       hostname
    2    Finished   2       hostname
    3    Finished   1       hostname
    4    Finished   1       hostname
    5    Finished   2       hostname
    6    Finished   3       hostname
    7    Finished   3       hostname
    8    Finished   3       hostname
    9    Finished   3       hostname
    10   Finished   3       hostname\r\necho hostname\r\nhostname
    11   Finished   3       hostname
    12   Finished   3       echo ??
    13   Finished   3       echo ??
    -------------------------------------------------------------
    Job count: 13

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job
    Id   State      Nodes   Command
    --   --------   -----   --------------------------------------------------
    1    Finished   40      hostname
    2    Finished   40      hostname
    3    Finished   40      hostname
    4    Canceled   40      ping localhost
    5    Finished   40      snap install docker
    6    Finished   40      docker version
    7    Finished   30      hostname
    8    Finished   30      hostname
    9    Finished   1       echo 1
    10   Finished   40      ping -c 4 localhost
    11   Finished   40      hostname
    12   Finished   40      ping -c 10 localhost
    13   Finished   40      hostname
    14   Finished   40      hostname
    15   Finished   40      headnode=vmss-ubun000000\ncd ~\ncp /usr/local/c...
    16   Failed     160     clus.run.20200414173133 s echo clus.run.2020041...
    17   Finished   160     echo ??
    18   Finished   160     echo ??
    19   Finished   160     echo ??
    20   Finished   160     echo ??
    21   Finished   160     echo ??
    22   Finished   160     hostname
    23   Finished   160     echo x
    24   Finished   160     echo x
    25   Finished   160     hostname
    26   Finished   160     hostname
    27   Canceled   40      ping localhost
    28   Failed     160     clusnode
    29   Finished   1       pwd
    30   Finished   1       ls
    31   Finished   1       /clusnode
    32   Failed     40      clusnode config add -headnodes win2016
    33   Finished   160     hostname
    34   Finished   160     hostname
    35   Finished   160     hostname
    36   Finished   160     hostname
    37   Finished   160     echo ??
    38   Failed     160     docker ps
    39   Finished   40      docker ps
    40   Finished   40      docker kill $(docker ps -q)\nheadnode=vmss-ubun...
    41   Finished   480     echo ??
    42   Finished   480     echo ??
    43   Failed     480     clusnode config get
    44   Finished   480     echo ??
    45   Finished   40      clusnode config get
    46   Finished   40      clusnode config add -headnodes win2016
    47   Finished   40      clusnode config get
    48   Finished   40      clusnode config get
    49   Finished   480     echo ??
    50   Finished   480     echo ??
    51   Finished   40      clusnode config get
    52   Finished   40      clusnode config del -headnodes win2016
    53   Finished   40      clusnode config get
    54   Finished   40      clusnode config get
    55   Finished   40      clusnode config add -headnodes win2016
    56   Finished   40      clusnode config get
    57   Finished   440     /clusnode config get
    58   Failed     40      headnode=win2016\nfor port in {60000..60010}\nd...
    59   Finished   40      headnode=win2016\nfor port in {60000..60010}\nd...
    60   Finished   40      for port in {60000..60010}\ndo\n        clusnode confi...
    61   Finished   469     hostname
    62   Finished   40      headnode=vmss-ubun000000\ncd ~\ncp /usr/local/c...
    63   Finished   908     hostname
    64   Finished   908     hostname
    65   Finished   908     hostname
    66   Finished   908     hostname
    67   Finished   40      docker
    68   Finished   40      echo
    69   Finished   40      echo $(docker ps -q)
    70   Finished   40      docker kill $(docker ps -q)
    71   Finished   40      clusnode config set -headnodes VMSS-UBUN000001
    72   Finished   3       hostname
    73   Finished   2       hostname
    74   Finished   2       hostname
    75   Finished   2       hostname
    76   Finished   3       hostname
    77   Finished   3       hostname
    78   Finished   3       hostname
    79   Finished   3       hostname
    80   Finished   3       hostname\necho 1\nhostname\n
    81   Finished   3       hostname
    82   Finished   3       echo ??
    83   Finished   3       echo ??
    --------------------------------------------------------------------------
    Job count: 83
    root@vmss-ubun000000:~#
    ```
    </details>

- List specified jobs

    ```CMD or Bash
    clus job <[~]id[-id][,[~]id[-id]]> [[~]id[-id][,[~]id[-id]]]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job 1,3-5 7 ~1-2
    Id                  : 1
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:41:41 +0000 GMT
    End Time            : 2020-04-15 17:41:41 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 3
    State               : Finished
    Nodes               : WIN2016
    Create Time         : 2020-04-15 17:44:13 +0000 GMT
    End Time            : 2020-04-15 17:44:13 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 4
    State               : Finished
    Nodes               : WIN2016
    Create Time         : 2020-04-15 17:44:45 +0000 GMT
    End Time            : 2020-04-15 17:44:45 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 5
    State               : Finished
    Nodes               : VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:46:26 +0000 GMT
    End Time            : 2020-04-15 17:46:26 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 7
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:47:50 +0000 GMT
    End Time            : 2020-04-15 17:47:51 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 12
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:57:39 +0000 GMT
    End Time            : 2020-04-15 17:57:39 +0000 GMT
    Sweep Parameter     : ??
    Command             : echo ??
    ----------------------------------------------------------------------------
    Id                  : 13
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:57:44 +0000 GMT
    End Time            : 2020-04-15 17:57:44 +0000 GMT
    Sweep Parameter     : ??{10-9:-1}
    Command             : echo ??
    ----------------------------------------------------------------------------
    Job count: 7

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job 1,3-5 7 ~1-2
    Id                  : 1
    State               : Finished
    Nodes               : VMSS-UBUN000000, VMSS-UBUN000001, VMSS-UBUN000002, VMSS-UBUN000003, VMSS-UBUN000004, VMSS-UBUN000005, VMSS-UBUN000006, VMSS-UBUN000007, VMSS-UBUN000008, VMSS-UBUN000009, VMSS-UBUN00000C, VMSS-UBUN00000D, VMSS-UBUN00000E, VMSS-UBUN00000F, VMSS-UBUN00000G, VMSS-UBUN00000H, VMSS-UBUN00000J, VMSS-UBUN00000M, VMSS-UBUN00000N, VMSS-UBUN00000O, VMSS-UBUN00000P, VMSS-UBUN00000Q, VMSS-UBUN00000S, VMSS-UBUN00000T, VMSS-UBUN00000U, VMSS-UBUN00000V, VMSS-UBUN00000W, VMSS-UBUN00000X, VMSS-UBUN00000Y, VMSS-UBUN00000Z, VMSS-UBUN000010, VMSS-UBUN000011, VMSS-UBUN000012, VMSS-UBUN000013, VMSS-UBUN000014, VMSS-UBUN000015, VMSS-UBUN000016, VMSS-UBUN000019, VMSS-UBUN00001A, VMSS-UBUN00001B
    Create Time         : 2020-04-14 17:03:33 +0000 UTC
    End Time            : 2020-04-14 17:03:33 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 3
    State               : Finished
    Nodes               : VMSS-UBUN000000, VMSS-UBUN000001, VMSS-UBUN000002, VMSS-UBUN000003, VMSS-UBUN000004, VMSS-UBUN000005, VMSS-UBUN000006, VMSS-UBUN000007, VMSS-UBUN000008, VMSS-UBUN000009, VMSS-UBUN00000C, VMSS-UBUN00000D, VMSS-UBUN00000E, VMSS-UBUN00000F, VMSS-UBUN00000G, VMSS-UBUN00000H, VMSS-UBUN00000J, VMSS-UBUN00000M, VMSS-UBUN00000N, VMSS-UBUN00000O, VMSS-UBUN00000P, VMSS-UBUN00000Q, VMSS-UBUN00000S, VMSS-UBUN00000T, VMSS-UBUN00000U, VMSS-UBUN00000V, VMSS-UBUN00000W, VMSS-UBUN00000X, VMSS-UBUN00000Y, VMSS-UBUN00000Z, VMSS-UBUN000010, VMSS-UBUN000011, VMSS-UBUN000012, VMSS-UBUN000013, VMSS-UBUN000014, VMSS-UBUN000015, VMSS-UBUN000016, VMSS-UBUN000019, VMSS-UBUN00001A, VMSS-UBUN00001B
    Create Time         : 2020-04-14 17:19:24 +0000 UTC
    End Time            : 2020-04-14 17:19:24 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 4
    State               : Canceled
    Nodes               : VMSS-UBUN000000, VMSS-UBUN000001, VMSS-UBUN000002, VMSS-UBUN000003, VMSS-UBUN000004, VMSS-UBUN000005, VMSS-UBUN000006, VMSS-UBUN000007, VMSS-UBUN000008, VMSS-UBUN000009, VMSS-UBUN00000C, VMSS-UBUN00000D, VMSS-UBUN00000E, VMSS-UBUN00000F, VMSS-UBUN00000G, VMSS-UBUN00000H, VMSS-UBUN00000J, VMSS-UBUN00000M, VMSS-UBUN00000N, VMSS-UBUN00000O, VMSS-UBUN00000P, VMSS-UBUN00000Q, VMSS-UBUN00000S, VMSS-UBUN00000T, VMSS-UBUN00000U, VMSS-UBUN00000V, VMSS-UBUN00000W, VMSS-UBUN00000X, VMSS-UBUN00000Y, VMSS-UBUN00000Z, VMSS-UBUN000010, VMSS-UBUN000011, VMSS-UBUN000012, VMSS-UBUN000013, VMSS-UBUN000014, VMSS-UBUN000015, VMSS-UBUN000016, VMSS-UBUN000019, VMSS-UBUN00001A, VMSS-UBUN00001B
    Create Time         : 2020-04-14 17:21:25 +0000 UTC
    End Time            : 2020-04-14 17:21:55 +0000 UTC
    Failed Nodes        : VMSS-UBUN000000 -> -1, VMSS-UBUN000001 -> -1, VMSS-UBUN000002 -> -1, VMSS-UBUN000003 -> -1, VMSS-UBUN000004 -> -1, VMSS-UBUN000005 -> -1, VMSS-UBUN000006 -> -1, VMSS-UBUN000007 -> -1, VMSS-UBUN000008 -> -1, VMSS-UBUN000009 -> -1, VMSS-UBUN00000C -> -1, VMSS-UBUN00000D -> -1, VMSS-UBUN00000E -> -1, VMSS-UBUN00000F -> -1, VMSS-UBUN00000G -> -1, VMSS-UBUN00000H -> -1, VMSS-UBUN00000J -> -1, VMSS-UBUN00000M -> -1, VMSS-UBUN00000N -> -1, VMSS-UBUN00000O -> -1, VMSS-UBUN00000P -> -1, VMSS-UBUN00000Q -> -1, VMSS-UBUN00000S -> -1, VMSS-UBUN00000T -> -1, VMSS-UBUN00000U -> -1, VMSS-UBUN00000V -> -1, VMSS-UBUN00000W -> -1, VMSS-UBUN00000X -> -1, VMSS-UBUN00000Y -> -1, VMSS-UBUN00000Z -> -1, VMSS-UBUN000010 -> -1, VMSS-UBUN000011 -> -1, VMSS-UBUN000012 -> -1, VMSS-UBUN000013 -> -1, VMSS-UBUN000014 -> -1, VMSS-UBUN000015 -> -1, VMSS-UBUN000016 -> -1, VMSS-UBUN000019 -> -1, VMSS-UBUN00001A -> -1, VMSS-UBUN00001B -> -1
    Command             : ping localhost
    --------------------------------------------------------------------------
    Id                  : 5
    State               : Finished
    Nodes               : VMSS-UBUN000000, VMSS-UBUN000001, VMSS-UBUN000002, VMSS-UBUN000003, VMSS-UBUN000004, VMSS-UBUN000005, VMSS-UBUN000006, VMSS-UBUN000007, VMSS-UBUN000008, VMSS-UBUN000009, VMSS-UBUN00000C, VMSS-UBUN00000D, VMSS-UBUN00000E, VMSS-UBUN00000F, VMSS-UBUN00000G, VMSS-UBUN00000H, VMSS-UBUN00000J, VMSS-UBUN00000M, VMSS-UBUN00000N, VMSS-UBUN00000O, VMSS-UBUN00000P, VMSS-UBUN00000Q, VMSS-UBUN00000S, VMSS-UBUN00000T, VMSS-UBUN00000U, VMSS-UBUN00000V, VMSS-UBUN00000W, VMSS-UBUN00000X, VMSS-UBUN00000Y, VMSS-UBUN00000Z, VMSS-UBUN000010, VMSS-UBUN000011, VMSS-UBUN000012, VMSS-UBUN000013, VMSS-UBUN000014, VMSS-UBUN000015, VMSS-UBUN000016, VMSS-UBUN000019, VMSS-UBUN00001A, VMSS-UBUN00001B
    Create Time         : 2020-04-14 17:23:11 +0000 UTC
    End Time            : 2020-04-14 17:23:49 +0000 UTC
    Command             : snap install docker
    --------------------------------------------------------------------------
    Id                  : 7
    State               : Finished
    Nodes               : VMSS-UBUN000000, VMSS-UBUN000001, VMSS-UBUN000002, VMSS-UBUN000003, VMSS-UBUN000004, VMSS-UBUN000005, VMSS-UBUN000006, VMSS-UBUN000007, VMSS-UBUN000008, VMSS-UBUN000009, VMSS-UBUN00000C, VMSS-UBUN00000D, VMSS-UBUN00000E, VMSS-UBUN00000F, VMSS-UBUN00000G, VMSS-UBUN00000H, VMSS-UBUN00000J, VMSS-UBUN00000M, VMSS-UBUN00000N, VMSS-UBUN00000O, VMSS-UBUN00000P, VMSS-UBUN00000Q, VMSS-UBUN00000S, VMSS-UBUN00000T, VMSS-UBUN00000U, VMSS-UBUN00000V, VMSS-UBUN00000W, VMSS-UBUN00000X, VMSS-UBUN00000Y, VMSS-UBUN00000Z
    Create Time         : 2020-04-14 17:24:50 +0000 UTC
    End Time            : 2020-04-14 17:24:51 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 82
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:55:37 +0000 UTC
    End Time            : 2020-04-15 17:55:37 +0000 UTC
    Sweep Parameter     : ??
    Command             : echo ??
    --------------------------------------------------------------------------
    Id                  : 83
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:56:28 +0000 UTC
    End Time            : 2020-04-15 17:56:28 +0000 UTC
    Sweep Parameter     : ??{10-9:-1}
    Command             : echo ??
    --------------------------------------------------------------------------
    Job count: 7
    root@vmss-ubun000000:~#
    ```
    </details>

- List jobs with different displaying format

    ```CMD or Bash
    clus job -format <table/list>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job -format table ~1-10
    Id   State      Nodes   Command
    --   --------   -----   -------------------------------------
    4    Finished   1       hostname
    5    Finished   2       hostname
    6    Finished   3       hostname
    7    Finished   3       hostname
    8    Finished   3       hostname
    9    Finished   3       hostname
    10   Finished   3       hostname\r\necho hostname\r\nhostname
    11   Finished   3       hostname
    12   Finished   3       echo ??
    13   Finished   3       echo ??
    -------------------------------------------------------------
    Job count: 10

    c:\>clus job -format list ~1-10
    Id                  : 4
    State               : Finished
    Nodes               : WIN2016
    Create Time         : 2020-04-15 17:44:45 +0000 GMT
    End Time            : 2020-04-15 17:44:45 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 5
    State               : Finished
    Nodes               : VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:46:26 +0000 GMT
    End Time            : 2020-04-15 17:46:26 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 6
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:46:55 +0000 GMT
    End Time            : 2020-04-15 17:46:55 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 7
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:47:50 +0000 GMT
    End Time            : 2020-04-15 17:47:51 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 8
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:48:43 +0000 GMT
    End Time            : 2020-04-15 17:48:43 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 9
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:49:18 +0000 GMT
    End Time            : 2020-04-15 17:49:18 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 10
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:52:05 +0000 GMT
    End Time            : 2020-04-15 17:52:05 +0000 GMT
    Command             : hostname
    echo hostname
    hostname
    ----------------------------------------------------------------------------
    Id                  : 11
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:53:33 +0000 GMT
    End Time            : 2020-04-15 17:53:33 +0000 GMT
    Command             : hostname
    ----------------------------------------------------------------------------
    Id                  : 12
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:57:39 +0000 GMT
    End Time            : 2020-04-15 17:57:39 +0000 GMT
    Sweep Parameter     : ??
    Command             : echo ??
    ----------------------------------------------------------------------------
    Id                  : 13
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:57:44 +0000 GMT
    End Time            : 2020-04-15 17:57:44 +0000 GMT
    Sweep Parameter     : ??{10-9:-1}
    Command             : echo ??
    ----------------------------------------------------------------------------
    Job count: 10

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job -format table ~1-10
    Id   State      Nodes   Command
    --   --------   -----   ----------------------------
    74   Finished   2       hostname
    75   Finished   2       hostname
    76   Finished   3       hostname
    77   Finished   3       hostname
    78   Finished   3       hostname
    79   Finished   3       hostname
    80   Finished   3       hostname\necho 1\nhostname\n
    81   Finished   3       hostname
    82   Finished   3       echo ??
    83   Finished   3       echo ??
    ----------------------------------------------------
    Job count: 10
    root@vmss-ubun000000:~# clus job -format list ~1-10
    Id                  : 74
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000
    Create Time         : 2020-04-15 17:43:33 +0000 UTC
    End Time            : 2020-04-15 17:43:33 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 75
    State               : Finished
    Nodes               : VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:45:41 +0000 UTC
    End Time            : 2020-04-15 17:45:41 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 76
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:47:02 +0000 UTC
    End Time            : 2020-04-15 17:47:02 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 77
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:47:26 +0000 UTC
    End Time            : 2020-04-15 17:47:26 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 78
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:48:51 +0000 UTC
    End Time            : 2020-04-15 17:48:52 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 79
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:49:44 +0000 UTC
    End Time            : 2020-04-15 17:49:44 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 80
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:52:57 +0000 UTC
    End Time            : 2020-04-15 17:52:57 +0000 UTC
    Command             : hostname
    echo 1
    hostname

    --------------------------------------------------------------------------
    Id                  : 81
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:54:04 +0000 UTC
    End Time            : 2020-04-15 17:54:04 +0000 UTC
    Command             : hostname
    --------------------------------------------------------------------------
    Id                  : 82
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:55:37 +0000 UTC
    End Time            : 2020-04-15 17:55:37 +0000 UTC
    Sweep Parameter     : ??
    Command             : echo ??
    --------------------------------------------------------------------------
    Id                  : 83
    State               : Finished
    Nodes               : 67A226CD8E76(VMSS-UBUN000000:60000), VMSS-UBUN000000, WIN2016
    Create Time         : 2020-04-15 17:56:28 +0000 UTC
    End Time            : 2020-04-15 17:56:28 +0000 UTC
    Sweep Parameter     : ??{10-9:-1}
    Command             : echo ??
    --------------------------------------------------------------------------
    Job count: 10
    root@vmss-ubun000000:~#
    ```
    </details>

- Cancel jobs

    ```CMD or Bash
    clus job -cancel <[~]id[-id][,[~]id[-id]]> [[~]id[-id][,[~]id[-id]]]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus run -background ping -t localhost
    Job 14 started on 3 nodes in cluster localhost:50505.

    c:\>clus job -cancel ~~
    Canceling jobs: [14]

    c:\>clus job -cancel ~~
    Canceled jobs: [14]

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -background ping localhost
    Job 84 started on 3 nodes in cluster localhost:50505.
    root@vmss-ubun000000:~# clus job -cancel ~~
    Canceling jobs: [84]
    root@vmss-ubun000000:~# clus job -cancel ~~
    Canceled jobs: [84]
    root@vmss-ubun000000:~#
    ```
    </details>

- Rerun jobs

    ```CMD or Bash
    clus job -rerun <[~]id[-id][,[~]id[-id]]> [[~]id[-id][,[~]id[-id]]]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job -rerun ~~
    Rerun job 14: Job 15 started on 3 nodes in cluster localhost:50505.

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job -rerun ~~
    Rerun job 84: Job 85 started on 3 nodes in cluster localhost:50505.
    root@vmss-ubun000000:~#
    ```
    </details>

- List jobs in specified cluster

    ```CMD or Bash
    clus job -headnode <node host>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clus job -headnode VMSS-UBUN000000 -format table ~1-10
    Id   State      Nodes   Command
    --   --------   -----   ----------------------------
    76   Finished   3       hostname
    77   Finished   3       hostname
    78   Finished   3       hostname
    79   Finished   3       hostname
    80   Finished   3       hostname\necho 1\nhostname\n
    81   Finished   3       hostname
    82   Finished   3       echo ??
    83   Finished   3       echo ??
    84   Canceled   3       ping localhost
    85   Canceled   3       ping localhost
    ----------------------------------------------------
    Job count: 10

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clus job -headnode win2016
    Id   State      Nodes   Command
    --   --------   -----   -------------------------------------
    1    Finished   3       hostname
    2    Finished   2       hostname
    3    Finished   1       hostname
    4    Finished   1       hostname
    5    Finished   2       hostname
    6    Finished   3       hostname
    7    Finished   3       hostname
    8    Finished   3       hostname
    9    Finished   3       hostname
    10   Finished   3       hostname\r\necho hostname\r\nhostname
    11   Finished   3       hostname
    12   Finished   3       echo ??
    13   Finished   3       echo ??
    14   Canceled   3       ping -t localhost
    15   Canceled   3       ping -t localhost
    -------------------------------------------------------------
    Job count: 15
    root@vmss-ubun000000:~#
    ```
    </details>

</details>

### Worker executable `clusnode` or `clusnode.exe`

- Show usage

    ```CMD or Bash
    clusnode
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the node
            config          - configure the started node

    Usage of start:
            clusnode start [options]
            clusnode start -h

    Usage of config:
            clusnode config <command> [configs]
            clusnode config -h


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the node
            config          - configure the started node

    Usage of start:
            clusnode start [options]
            clusnode start -h

    Usage of config:
            clusnode config <command> [configs]
            clusnode config -h

    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clusnode -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode -h

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the node
            config          - configure the started node

    Usage of start:
            clusnode start [options]
            clusnode start -h

    Usage of config:
            clusnode config <command> [configs]
            clusnode config -h


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode -h

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the node
            config          - configure the started node

    Usage of start:
            clusnode start [options]
            clusnode start -h

    Usage of config:
            clusnode config <command> [configs]
            clusnode config -h

    root@vmss-ubun000000:~#
    ```
    </details>

</details>

<details><summary>clusnode start</summary>

#### Start node by command `clusnode start`

- Show usage

    ```CMD or Bash
    clusnode start -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode start -h
    Usage of clusnode start options:
    -config-file string
            specify the config file for saving and loading settings (default "C:\\windows\\clusnode.exe.config")
    -headnodes string
            specify the host addresses of headnodes for this clusnode to join in
    -host string
            specify the host address of this headnode and clusnode (default "WIN2016:50505")
    -log-file string
            specify the file for logging (default "C:\\windows\\clusnode.exe.logs\\<host>.<start time>.log")
    -pprof
            start HTTP server on 0.0.0.0:8080 for pprof

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode start -h
    Usage of clusnode start options:
    -config-file string
            specify the config file for saving and loading settings (default "/usr/local/clusrun/clusnode.config")
    -headnodes string
            specify the host addresses of headnodes for this clusnode to join in
    -host string
            specify the host address of this headnode and clusnode (default "VMSS-UBUN000000:50505")
    -log-file string
            specify the file for logging (default "/usr/local/clusrun/clusnode.logs/<host>.<start time>.log")
    -pprof
            start HTTP server on 0.0.0.0:8080 for pprof
    root@vmss-ubun000000:~#
    ```
    </details>

- Start node with default parameters

    ```CMD or Bash
    clusnode start
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start
    Log file: D:\clusnode.exe.logs\WIN2016.50505.20200415182515.log
    Service is being initialized
    Service initialized
    Service started with pid 6792

    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start
    Log file: /usr/local/clusrun/clusnode.logs/VMSS-UBUN000000.50505.20200415182524.log
    Service is being initialized
    Service initialized
    Service started with pid 121017

    ```
    </details>

- Start node with specified log file

    ```CMD or Bash
    clusnode start -log-file <file>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start -log-file log.txt
    Log file: log.txt
    Service is being initialized
    Service initialized
    Service started with pid 2400
    Service is stopping
    Service stopped

    D:\>more log.txt
    2020/04/15 18:35:17.882378 | Info | Config file: D:\clusnode.exe.config
    2020/04/15 18:35:17.916401 | Info | Loading node configs
    2020/04/15 18:35:17.916401 | Info | Config file doesn't exist, use default configs
    2020/04/15 18:35:17.916401 | Info | Adding default headnode: WIN2016:50505
    2020/04/15 18:35:17.916401 | Info | Saving node configs
    2020/04/15 18:35:17.916401 | Info | Start heartbeat from WIN2016:50505 to WIN2016:50505
    2020/04/15 18:35:17.917364 | Info | Config file doesn't exist, build it
    2020/04/15 18:35:17.918365 | Info | Setup in-bound firewall of port 50505
    2020/04/15 18:35:17.918365 | Info | Run command: netsh advfirewall firewall delete rule name=clusnode-50505
    2020/04/15 18:35:17.973454 | Info | Command output:
    Deleted 1 rule(s).
    Ok.

    2020/04/15 18:35:17.973454 | Info | Run command: netsh advfirewall firewall add rule name=clusnode-50505 localport=50505 dir=in protocol=tcp enable=yes action=allow profile=private,domain,public
    2020/04/15 18:35:18.023375 | Info | Command output: Ok.

    2020/04/15 18:35:18.023375 | Info | Initializing database
    2020/04/15 18:35:18.024341 | Info | Node WIN2016 starts listening on WIN2016:50505
    2020/04/15 18:35:18.228663 | Info | Connected to headnode WIN2016:50505
    2020/04/15 18:35:18.230726 | Info | First heartbeat from WIN2016
    2020/04/15 18:35:18.230726 | Info | Start validating clusnode WIN2016
    2020/04/15 18:35:18.231690 | Info | Connected to clusnode host WIN2016:50505
    2020/04/15 18:35:18.231690 | Info | Received validation request from WIN2016:50505 to WIN2016:50505
    2020/04/15 18:35:18.231690 | Info | Clusnode WIN2016 is validated that being hosted by WIN2016:50505
    2020/04/15 18:35:20.178165 | Info | Exited

    D:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start -log-file log
    Log file: log
    Service is being initialized
    Service initialized
    Service started with pid 121187
    ^CService is stopping
    Service stopped
    root@vmss-ubun000000:~/clusrun# more log | head
    2020/04/15 18:26:22 | Info | Config file: /usr/local/clusrun/clusnode.config
    2020/04/15 18:26:22 | Info | Loading node configs
    2020/04/15 18:26:22 | Info | Adding loaded headnodes: [WIN2016:50505 VMSS-UBUN000001:50505 VMSS-UBUN000000:50505]
    2020/04/15 18:26:22 | Info | Set config "heartbeat interval in seconds" to 1
    2020/04/15 18:26:22 | Info | Set config "mark node lost after no heartbeat for seconds" to 5
    2020/04/15 18:26:22 | Info | Set config "max job count" to 100
    2020/04/15 18:26:22 | Info | Set config "store output" to true
    2020/04/15 18:26:22 | Info | Set config "add go id in logs" to false
    2020/04/15 18:26:22 | Info | Saving node configs
    2020/04/15 18:26:22 | Info | Start heartbeat from VMSS-UBUN000000:50505 to WIN2016:50505
    root@vmss-ubun000000:~/clusrun#
    ```
    </details>

- Start node with specified config file

    ```CMD or Bash
    clusnode start -config-file <file>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start -config-file config.txt
    Log file: D:\clusnode.exe.logs\WIN2016.50505.20200415182701.log
    Service is being initialized
    Service initialized
    Service started with pid 3504
    Service is stopping
    Service stopped

    D:\>more config.txt
    {
        "WIN2016:50505": {
            "add go id in logs": false,
            "clusnode role": {
                "headnodes": [
                    "WIN2016:50505"
                ],
                "heartbeat interval in seconds": 1
            },
            "headnode role": {
                "mark node lost after no heartbeat for seconds": 5,
                "max job count": 100,
                "store output": true
            }
        }
    }

    D:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start -config-file config
    Log file: /usr/local/clusrun/clusnode.logs/VMSS-UBUN000000.50505.20200415183823.log
    Service is being initialized
    Service initialized
    Service started with pid 121848
    ^CService is stopping
    Service stopped
    root@vmss-ubun000000:~/clusrun# more config
    {
        "VMSS-UBUN000000:50505": {
            "add go id in logs": false,
            "clusnode role": {
                "headnodes": [
                    "VMSS-UBUN000000:50505"
                ],
                "heartbeat interval in seconds": 1
            },
            "headnode role": {
                "mark node lost after no heartbeat for seconds": 5,
                "max job count": 100,
                "store output": true
            }
        }
    }
    root@vmss-ubun000000:~/clusrun#
    ```
    </details>

- Start node with specified host address

    ```CMD or Bash
    clusnode start -host <hostname[:port]>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start -host localhost:60000
    Log file: D:\clusnode.exe.logs\WIN2016.60000.20200415184014.log
    Service is being initialized
    Service initialized
    Service started with pid 1720

    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start -host localhost:60000
    Log file: /usr/local/clusrun/clusnode.logs/VMSS-UBUN000000.60000.20200415184111.log
    Service is being initialized
    Service initialized
    Service started with pid 122126

    ```
    </details>

- Start node joining specified headnodes

    ```CMD or Bash
    clusnode start -headnodes <node hosts>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start -headnodes VMSS-UBUN000000:60000
    Log file: D:\clusnode.exe.logs\WIN2016.50505.20200415184208.log
    Service is being initialized
    Service initialized
    Service started with pid 6420
    Service is stopping
    Service stopped

    D:\>clus node -headnode VMSS-UBUN000000:60000
    Node                                     State
    --------------------------------------   -----
    VMSS-UBUN000000(VMSS-UBUN000000:60000)   Ready
    WIN2016                                  Lost
    ----------------------------------------------
    Node count: 2

    D:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start -headnodes win2016:60000    
    Log file: /usr/local/clusrun/clusnode.logs/VMSS-UBUN000000.50505.20200415184308.log
    Service is being initialized
    Service initialized
    Service started with pid 122238
    ^CService is stopping
    Service stopped
    root@vmss-ubun000000:~/clusrun# clus node -headnode win2016
    Node              State
    ---------------   -----
    VMSS-UBUN000000   Lost
    WIN2016           Ready
    -----------------------
    Node count: 2
    root@vmss-ubun000000:~/clusrun#
    ```
    </details>

- Start node with pprof server for debugging

    ```CMD or Bash
    clusnode start -pprof
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    D:\>clusnode start -pprof
    Log file: D:\clusnode.exe.logs\WIN2016.50505.20200415184518.log
    Service is being initialized
    Service initialized
    Service started with pid 5436

    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~/clusrun# clusnode start -pprof &
    [1] 122593
    root@vmss-ubun000000:~/clusrun# Log file: /usr/local/clusrun/clusnode.logs/VMSS-UBUN000000.50505.20200415184625.log
    Service is being initialized
    Service initialized
    Service started with pid 122593
    curl http://localhost:8080/debug/pprof/
    <html>
    <head>
    <title>/debug/pprof/</title>
    <style>
    .profile-name{
            display:inline-block;
            width:6rem;
    }
    </style>
    </head>
    <body>
    /debug/pprof/<br>
    <br>
    Types of profiles available:
    <table>
    <thead><td>Count</td><td>Profile</td></thead>

            <tr>
            <td>55</td><td><a href=allocs?debug&#61;1>allocs</a></td>
            </tr>

            <tr>
            <td>0</td><td><a href=block?debug&#61;1>block</a></td>
            </tr>

            <tr>
            <td>0</td><td><a href=cmdline>cmdline</a></td>
            </tr>

            <tr>
            <td>13</td><td><a href=goroutine?debug&#61;1>goroutine</a></td>
            </tr>

            <tr>
            <td>55</td><td><a href=heap?debug&#61;1>heap</a></td>
            </tr>

            <tr>
            <td>0</td><td><a href=mutex?debug&#61;1>mutex</a></td>
            </tr>

            <tr>
            <td>0</td><td><a href=profile>profile</a></td>
            </tr>

            <tr>
            <td>8</td><td><a href=threadcreate?debug&#61;1>threadcreate</a></td>
            </tr>

            <tr>
            <td>0</td><td><a href=trace>trace</a></td>
            </tr>

    </table>
    <a href="goroutine?debug=2">full goroutine stack dump</a>
    <br/>
    <p>
    Profile Descriptions:
    <ul>

    <li><div class=profile-name>allocs:</div> A sampling of all past memory allocations</li>

    <li><div class=profile-name>block:</div> Stack traces that led to blocking on synchronization primitives</li>

    <li><div class=profile-name>cmdline:</div> The command line invocation of the current program</li>

    <li><div class=profile-name>goroutine:</div> Stack traces of all current goroutines</li>

    <li><div class=profile-name>heap:</div> A sampling of memory allocations of live objects. You can specify the gc GET parameter to run GC before taking the heap sample.</li>

    <li><div class=profile-name>mutex:</div> Stack traces of holders of contended mutexes</li>

    <li><div class=profile-name>profile:</div> CPU profile. You can specify the duration in the seconds GET parameter. After you get the profile file, use the go tool pprof command to investigate the profile.</li>

    <li><div class=profile-name>threadcreate:</div> Stack traces that led to the creation of new OS threads</li>

    <li><div class=profile-name>trace:</div> A trace of execution of the current program. You can specify the duration in the seconds GET parameter. After you get the trace file, use the go tool trace command to investigate the trace.</li>

    </ul>
    </p>
    </body>
    </html>
    root@vmss-ubun000000:~/clusrun#
    ```
    </details>

</details>

<details><summary>clusnode config</summary>

#### Configure node by command `clusnode config`

- Show usage

    ```CMD or Bash
    clusnode config
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config

    Usage:
            clusnode config <command> [configs]

    The commands are:
            add           - add headnodes for clusnode role
            del           - delete headnodes for clusnode role
            set           - set the configs for clusnode role and headnode role
            get           - get the configs for clusnode role and headnode role


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config

    Usage:
            clusnode config <command> [configs]

    The commands are:
            add           - add headnodes for clusnode role
            del           - delete headnodes for clusnode role
            set           - set the configs for clusnode role and headnode role
            get           - get the configs for clusnode role and headnode role

    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clusnode config -h
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config -h

    Usage:
            clusnode config <command> [configs]

    The commands are:
            add           - add headnodes for clusnode role
            del           - delete headnodes for clusnode role
            set           - set the configs for clusnode role and headnode role
            get           - get the configs for clusnode role and headnode role


    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config -h

    Usage:
            clusnode config <command> [configs]

    The commands are:
            add           - add headnodes for clusnode role
            del           - delete headnodes for clusnode role
            set           - set the configs for clusnode role and headnode role
            get           - get the configs for clusnode role and headnode role

    root@vmss-ubun000000:~#
    ```
    </details>

- Get configs of default node

    ```CMD or Bash
    clusnode config get
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config get
    Get clusnode role configs result:
            "(connected) headnodes": VMSS-UBUN000000:50505, WIN2016:50505
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "max job count": 100
            "store output": true
            "mark node lost after no heartbeat for seconds": 5

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config get
    Get clusnode role configs result:
            "(connected) headnodes": WIN2016:50505, VMSS-UBUN000001:50505, VMSS-UBUN000000:50505
            "(connecting) headnodes": WIN2016:60000
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "store output": true
            "mark node lost after no heartbeat for seconds": 5
            "max job count": 100
    root@vmss-ubun000000:~#
    ```
    </details>

- Set configs of default node

    ```CMD or Bash
    clusnode config set
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config set
    -headnodes string
            set headnodes for this clusnode to join in
    -heartbeat-interval string
            set the heartbeat interval of this clusnode
    -heartbeat-timeout string
            set the heartbeat timeout of this headnode
    -max-job-count string
            set the count of jobs to keep in history on this headnode
    -node string
            specify the node to config (default "WIN2016:50505")
    -store-output string
            set if store job output on this headnode

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config set
    -headnodes string
            set headnodes for this clusnode to join in
    -heartbeat-interval string
            set the heartbeat interval of this clusnode
    -heartbeat-timeout string
            set the heartbeat timeout of this headnode
    -max-job-count string
            set the count of jobs to keep in history on this headnode
    -node string
            specify the node to config (default "VMSS-UBUN000000:50505")
    -store-output string
            set if store job output on this headnode
    root@vmss-ubun000000:~#
    ```
    </details>

    ```CMD or Bash
    clusnode config set [-headnodes <node hosts>] [-heartbeat-interval <number>] [-heartbeat-timeout <number>] [-max-job-count <number>] [-store-output <bool>]
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config set -heartbeat-timeout 10
    Set headnode role configs result:
            "mark node lost after no heartbeat for seconds": 10

    c:\>clusnode config get
    Get clusnode role configs result:
            "heartbeat interval in seconds": 1
            "(connected) headnodes": VMSS-UBUN000000:50505, WIN2016:50505
    Get headnode role configs result:
            "max job count": 100
            "store output": true
            "mark node lost after no heartbeat for seconds": 10

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config set -heartbeat-timeout 10
    Set headnode role configs result:
            "mark node lost after no heartbeat for seconds": 10
    root@vmss-ubun000000:~# clusnode config get
    Get clusnode role configs result:
            "(connected) headnodes": WIN2016:50505, VMSS-UBUN000001:50505, VMSS-UBUN000000:50505
            "(connecting) headnodes": WIN2016:60000
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "store output": true
            "mark node lost after no heartbeat for seconds": 10
            "max job count": 100
    root@vmss-ubun000000:~#
    ```
    </details>

- Add headnodes of default node

    ```CMD or Bash
    clusnode config add -headnodes <node hosts>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config add -headnodes test
    Set headnodes result:
            "TEST:50505": Added

    c:\>clusnode config get
    Get clusnode role configs result:
            "(connecting) headnodes": TEST:50505
            "heartbeat interval in seconds": 1
            "(connected) headnodes": VMSS-UBUN000000:50505, WIN2016:50505
    Get headnode role configs result:
            "mark node lost after no heartbeat for seconds": 10
            "max job count": 100
            "store output": true

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config add -headnodes test
    Set headnodes result:
            "TEST:50505": Added
    root@vmss-ubun000000:~# clusnode config get
    Get clusnode role configs result:
            "(connected) headnodes": WIN2016:50505, VMSS-UBUN000001:50505, VMSS-UBUN000000:50505
            "(connecting) headnodes": WIN2016:60000, TEST:50505
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "mark node lost after no heartbeat for seconds": 10
            "max job count": 100
            "store output": true
    root@vmss-ubun000000:~#
    ```
    </details>

- Remove headnodes of default node

    ```CMD or Bash
    clusnode config del -headnodes <node hosts>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config del -headnodes test
    Set headnodes result:
            "TEST:50505": Removed

    c:\>clusnode config get
    Get clusnode role configs result:
            "heartbeat interval in seconds": 1
            "(connected) headnodes": WIN2016:50505, VMSS-UBUN000000:50505
    Get headnode role configs result:
            "max job count": 100
            "store output": true
            "mark node lost after no heartbeat for seconds": 10

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config del -headnodes test
    Set headnodes result:
            "TEST:50505": Removed
    root@vmss-ubun000000:~# clusnode config get
    Get clusnode role configs result:
            "(connected) headnodes": VMSS-UBUN000001:50505, VMSS-UBUN000000:50505, WIN2016:50505
            "(connecting) headnodes": WIN2016:60000
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "mark node lost after no heartbeat for seconds": 10
            "max job count": 100
            "store output": true
    root@vmss-ubun000000:~#
    ```
    </details>

- Configure specified node

    ```CMD or Bash
    clusnode config <set/add/del/get> -node <node host>
    ```

    <details><summary>Example on Windows</summary>

    ```CMD
    c:\>clusnode config get -node vmss-ubun000000
    Get clusnode role configs result:
            "(connected) headnodes": VMSS-UBUN000000:50505, WIN2016:50505, VMSS-UBUN000001:50505
            "(connecting) headnodes": WIN2016:60000
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "max job count": 100
            "store output": true
            "mark node lost after no heartbeat for seconds": 10

    c:\>
    ```
    </details>

    <details><summary>Example on Linux</summary>

    ```Bash
    root@vmss-ubun000000:~# clusnode config get -node win2016
    Get clusnode role configs result:
            "(connected) headnodes": WIN2016:50505, VMSS-UBUN000000:50505
            "heartbeat interval in seconds": 1
    Get headnode role configs result:
            "max job count": 100
            "store output": true
            "mark node lost after no heartbeat for seconds": 10
    root@vmss-ubun000000:~#
    ```
    </details>

</details>

## Comparison and performance

### Latency

Measure the average time in client to run command `hostname` on nodes.

![chart-latency](images/clusrun-latency.png)

<details><summary>Data</summary>

| Approach | Latency (1 Windows VM) | Latency (100 Windows VMs) | Latency (500 Windows VMs) | Latency (1000 Windows VMs) | Latency (1500 Windows VMs) | Latency (1 Linux VM) | Latency (100 Linux VMs) | Latency (350 Linux VMs) | Latency (1000 Linux VMs) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| clusrun (Windows headnode) | 0.2s | 0.4s | 1.6s | 3.7s | 5.8s | 0.1s | 0.3s | 1.3s | 3.5s |
| clusrun (Linux headnode) | 0.1s | 0.2s | 10.6s* | 11.5s* | 18.5s* | 10ms | 50ms | 5.3s* | 11.1s* |
| [HPC Pack clusrun](https://docs.microsoft.com/en-us/powershell/high-performance-computing/clusrun) | 3.8s | 6.9s | 11.1s | - | - | 4s | 6.1s | 10s | - |
| [Azure VMSS Run Command via Azure CLI (Windows)](https://docs.microsoft.com/en-us/cli/azure/vmss/run-command?view=azure-cli-latest) | 159s | 45m | - | - | - | 67s | 19m | - | - |
| [Azure VMSS Run Command via Azure CLI (Linux)](https://docs.microsoft.com/en-us/cli/azure/vmss/run-command?view=azure-cli-latest) | 152s | 30m | - | - | - | 62s | 13m | - | - |
| [Azure VMSS Run Command via PowerShell](https://docs.microsoft.com/en-us/powershell/module/azurerm.compute/invoke-azurermvmssvmruncommand?view=azurermps-6.13.0) | 155s | N/A | N/A | N/A | N/A | 64s | N/A | N/A | N/A |
| [Azure VM Run Command via Azure portal](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/run-command) | 40s | N/A | N/A | N/A | N/A | 34s | N/A | N/A | N/A |

</details>

### Throughput

Start several jobs with command `hostname` from client in parallel and measure the total count of commands executed in certain time.

![chart-throughput-1](images/clusrun-throughput-1.png)

<details><summary>Environments</summary>

- VM Size: Standard_D4s_v3
- VM Disk: Standard SSD
- Widnows VM OS: Windows Server 2019 Datacenter
- Linux VM OS: Ubuntu Server 18.04 LTS
- Clusrun headnode role configs
    - "store output": false
    - "mark node lost after no heartbeat for seconds": 20
    - "max job count": 150

</details>

<details><summary>Data</summary>

| Approach | Throughput (commands per second) |
| --- | --- |
| HPC Pack clusrun (1 Windows headnode + 100 Windows compute nodes) | 76 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Windows compute nodes * 1 subscribed cores) | 158 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Windows compute nodes * 2 subscribed cores) | 212 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Windows compute nodes * 4 cores) | 263 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Windows compute nodes * 8 subscribed cores) | 285 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Windows compute nodes * 16 subscribed cores) | 259 |
| HPC Pack clusrun (1 Windows headnode + 100 Linux compute nodes) | 55 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Linux compute nodes * 1 subscribed cores) | 117 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Linux compute nodes * 2 subscribed cores) | 142 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Linux compute nodes * 4 cores) | 180 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Linux compute nodes * 8 subscribed cores) | 200 |
| HPC Pack parametric sweep job (1 Windows headnode + 100 Linux compute nodes * 16 subscribed cores) | 120 |
| clusrun (1 Windows headnode + 84 Windows VMs) | 768 |
| clusrun (2 Windows headnodes + 84 Windows VMs) | 1344 |
| clusrun (4 Windows headnodes + 84 Windows VMs) | 2389 |
| clusrun (8 Windows headnodes + 84 Windows VMs) | 3909 |
| clusrun (16 Windows headnodes + 84 Windows VMs) | 4527 |
| clusrun (1 Windows headnode + 84 Linux VMs) | 768 |
| clusrun (2 Windows headnodes + 84 Linux VMs) | 1344 |
| clusrun (4 Windows headnodes + 84 Linux VMs) | 2688 |
| clusrun (8 Windows headnodes + 84 Linux VMs) | 4779 |
| clusrun (16 Windows headnodes + 84 Linux VMs) | 8602 |
| clusrun (1 Linux headnode + 84 Windows VMs) | 1075 |
| clusrun (2 Linux headnodes + 84 Windows VMs) | 1792 |
| clusrun (4 Linux headnodes + 84 Windows VMs) | 3584 |
| clusrun (8 Linux headnodes + 84 Windows VMs) | 3909 |
| clusrun (16 Linux headnodes + 84 Windows VMs) | 4527 |
| clusrun (1 Linux headnode + 84 Linux VMs) | 1536 |
| clusrun (2 Linux headnodes + 84 Linux VMs) | 2688 |
| clusrun (4 Linux headnodes + 84 Linux VMs) | 5376 |
| clusrun (8 Linux headnodes + 84 Linux VMs) | 9557 |
| clusrun (16 Linux headnodes + 84 Linux VMs) | 19114 |
| clusrun (1 Windows headnode + 84 Linux VMs * 10 containers) | 1117 |
| clusrun (2 Windows headnodes + 84 Linux VMs * 10 containers) | 1490 |
| clusrun (4 Windows headnodes + 84 Linux VMs * 10 containers) | 2234 |
| clusrun (8 Windows headnodes + 84 Linux VMs * 10 containers) | 4469 |
| clusrun (16 Windows headnodes + 84 Linux VMs * 10 containers) | 10726 |
| clusrun (1 Linux headnode + 84 Linux VMs * 10 containers) | 1467 |
| clusrun (2 Linux headnodes + 84 Linux VMs * 10 containers) | 3300 |
| clusrun (4 Linux headnodes + 84 Linux VMs * 10 containers) | 7542 |
| clusrun (8 Linux headnodes + 84 Linux VMs * 10 containers) | 9600 |
| clusrun (16 Linux headnodes + 84 Linux VMs * 10 containers) | 21120 |

</details>

## Issues and solutions

### ARP cache limit

#### Environment

- Azure VMSS with 1000 instances as cluster nodes

- Ubuntu1804 as cluster headnode and client

#### Symptom

- Headnode can not be connected locally by `clus`

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# clus node
    Can not connect: context deadline exceeded
    Please ensure the headnode localhost:50505 is started and accessible
    ```
    </details>

- Headnode can be connected from other node

    <details><summary>Example</summary>

    ```PowerShell
    PS C:\Program Files\clusrun> .\clus.exe node -headnode ubuntu1804
    Node              State
    ---------------   -----
    ...               ...
    -----------------------
    Node count: 1001
    ```
    </details>

- Failure to ping localhost or other node from headnode

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# ping localhost
    ping: sendmsg: Invalid argument
    ping: sendmsg: Invalid argument
    ping: sendmsg: Invalid argument
    ...
    ```
    </details>

#### Solution

- Increase ARP cache limit, referenced to [Huang Huang 的博客](https://mozillazg.com/2017/10/linux-a-way-to-fix-haproxy-network-connection-timeout-ping-sendmsg-invalid-argument-socket-errno-110-connection-timed-out)

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# sysctl -a | grep net.ipv4.neigh.default.gc_thresh
    net.ipv4.neigh.default.gc_thresh1 = 128
    net.ipv4.neigh.default.gc_thresh2 = 512
    net.ipv4.neigh.default.gc_thresh3 = 1024
    ...
    root@ubuntu1804:~# sysctl -w net.ipv4.neigh.default.gc_thresh1=8192
    root@ubuntu1804:~# sysctl -w net.ipv4.neigh.default.gc_thresh2=8192
    root@ubuntu1804:~# sysctl -w net.ipv4.neigh.default.gc_thresh3=8192
    root@ubuntu1804:~# clus node
    Node              State
    ---------------   -----
    ...               ...
    -----------------------
    Node count: 1001
    ```
    </details>

### Open files limit

#### Environment

- Cluster containing 908 nodes

- Client OS: Ubuntu1804

#### Symptom

- Client fails to dump output files with error `too many open files` 

    <details><summary>Example</summary>

    ```Bash
    root@vmss-ubun000000:~# clus run -dump hostname
    Job 65 started on 908 nodes in cluster localhost:50505.
    Dumping output to /root/clus.run.20200415121609.040071600

    --------------------------Command---------------------------
    hostname
    ------------------------------------------------------------

    Failed to create output file: open /root/clus.run.20200415121609.040071600/95BF4307DCDB(VMSS-UBUN000010.60010).err: too many open files
    root@vmss-ubun000000:~#
    ```
    </details>

#### Solution

- Increase open files limit

    <details><summary>Example</summary>

    ```Bash
    root@vmss-ubun000000:~# ulimit -n
    1024
    root@vmss-ubun000000:~# ulimit -n 65536
    root@vmss-ubun000000:~# clus run -dump hostname
    Job 66 started on 908 nodes in cluster localhost:50505.
    Dumping output to /root/clus.run.20200415121820.093956800

    --------------------------Command---------------------------
    hostname
    ------------------------------------------------------------

    ...

    ------------------------------------------------------------
    Runtime:
    Min=429.7095ms
    Max=742.9697ms
    Mean=653.724117ms
    Mid=667.79725ms
    SD=63.521816ms

    908 of 908 nodes succeeded.
    Output is dumped to /root/clus.run.20200415121820.093956800
    root@vmss-ubun000000:~#
    ```
    </details>
