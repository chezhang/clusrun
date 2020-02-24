# clusrun

A simple tool to run commands in a cluster, inspired by the [clusrun](https://docs.microsoft.com/en-us/powershell/high-performance-computing/clusrun) feature from Microsoft HPC Pack.

## Components

![components](images/components.png)

- The executable `clusnode` or `clusnode.exe`

    - headnode role

        - receive config request from local client
        - receive node or job request from client
        - receive heartbeats from clusnodes and send node validatoin requests
        - dispatch job request to clusnodes and redirect responses
        - store job history locally

    - clusnode role

        - receive node validation or job request from headnode
        - send heartbeat to headnode
        - run command locally

    - client role

        - configure local headnode
        - configure local clusnode

- The executable `clus` or `clus.exe`

    - send node or job request to headnode and receive response
    - display node or job information locally

## Deployment

#### Install or uninstall clusrun as service

The substance of clusrun installation is to copy the setup package to a node and to register the extracted executable as a service, the uninstallation is to unregister the service and to delete files.

- Install and uninstall clusrun on physical or virtual machine

    1. Download setup package from [Github](https://github.com/chezhang/clusrun/releases) (`setup.zip` for Windows node, `setup.tar.gz` for Linux node)
    2. Extract it to the location you want to install clusrun
    3. Execute the install script with Administrator or root privilege to register clusrun as a service
    4. Execute the executable `clusnode` or `clusnode.exe` to set the headnode(s) for this node to report to
    5. Execute uninstall script to uninstall clusrun

    - The setup script on [Github](https://github.com/chezhang/clusrun/releases) (`setup.ps1` for Windows node, `setup.sh` for Linux node) can help to achieve the above steps

        <details><summary>Example on Windows</summary>
    
        ```PowerShell
        PS C:\> [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        PS C:\> (New-Object System.Net.WebClient).DownloadFile("https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.ps1", "$(pwd)\setup.ps1")
        PS C:\> .\setup.ps1 -headnodes $(hostname)
        02/23/2020 15:45:09  Setup clusrun: headnodes=win2019, location=C:\Program Files\clusrun, setup_url=https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.zip, reinstall=False, uninstall=False
        02/23/2020 15:45:09  Download clusrun from https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.zip
        02/23/2020 15:45:13  Extract clusrun from C:\\clusrun.setup.zip to C:\Program Files\clusrun
        02/23/2020 15:45:14  Install clusrun

        C:\Program Files\clusrun>sc stop clusnode
        [SC] OpenService FAILED 1060:

        The specified service does not exist as an installed service.


        C:\Program Files\clusrun>sc delete clusnode
        [SC] OpenService FAILED 1060:

        The specified service does not exist as an installed service.


        C:\Program Files\clusrun>sc create clusnode binpath= "C:\Program Files\clusrun\clusnode.exe start" start= auto
        [SC] CreateService SUCCESS

        C:\Program Files\clusrun>sc start clusnode

        SERVICE_NAME: clusnode
                TYPE               : 10  WIN32_OWN_PROCESS
                STATE              : 2  START_PENDING
                                        (NOT_STOPPABLE, NOT_PAUSABLE, IGNORES_SHUTDOWN)
                WIN32_EXIT_CODE    : 0  (0x0)
                SERVICE_EXIT_CODE  : 0  (0x0)
                CHECKPOINT         : 0x0
                WAIT_HINT          : 0x7d0
                PID                : 1436
                FLAGS              :
        02/23/2020 15:45:16  Set headnodes to win2019
        Result:
                [WIN2019:50505]:        Already connected
        PS C:\Program Files\clusrun>
        PS C:\Program Files\clusrun> .\clusnode.exe get
        WIN2019 is connected to headnodes: [WIN2019:50505]
        PS C:\Program Files\clusrun> .\clus.exe

        Usage:
                clus <command> [arguments]

        The commands are:
                node            - list node(s) in the cluster
                run             - run a command or script on node(s) in the cluster
                job             - list job(s) in the cluster

        Usage of node:
                clus node [options]
                clus node -h

        Usage of run:
                clus run [options] <command>
                clus run -h

        Usage of job:
                clus job [options] [jobs]
                clus job -h
        PS C:\Program Files\clusrun>
        PS C:\Program Files\clusrun> dir


            Directory: C:\Program Files\clusrun


        Mode                LastWriteTime         Length Name
        ----                -------------         ------ ----
        d-----        2/23/2020   3:45 PM                clusnode.exe.db
        d-----        2/23/2020   3:45 PM                clusnode.exe.log
        -a----         2/2/2020  12:24 PM       12535808 clus.exe
        -a----         2/2/2020  10:29 PM       13959168 clusnode.exe
        -a----        2/23/2020   3:45 PM             93 clusnode.exe.config
        -a----         2/2/2020  12:58 PM            258 uninstall.bat


        PS C:\Program Files\clusrun> .\uninstall.bat

        C:\Program Files\clusrun>sc stop clusnode

        SERVICE_NAME: clusnode
                TYPE               : 10  WIN32_OWN_PROCESS
                STATE              : 3  STOP_PENDING
                                        (NOT_STOPPABLE, NOT_PAUSABLE, IGNORES_SHUTDOWN)
                WIN32_EXIT_CODE    : 0  (0x0)
                SERVICE_EXIT_CODE  : 0  (0x0)
                CHECKPOINT         : 0x0
                WAIT_HINT          : 0x0

        C:\Program Files\clusrun>sc delete clusnode
        [SC] DeleteService SUCCESS

        C:\Program Files\clusrun>rmdir /Q /S "C:\Program Files\clusrun\clusnode.exe.db"

        C:\Program Files\clusrun>rmdir /Q /S "C:\Program Files\clusrun\clusnode.exe.log"

        C:\Program Files\clusrun>del "C:\Program Files\clusrun\clusnode.exe.config"

        C:\Program Files\clusrun>ping 127.0.0.1 -n 2  1>nul

        C:\Program Files\clusrun>del "C:\Program Files\clusrun\clusnode.exe"

        C:\Program Files\clusrun>del "C:\Program Files\clusrun\clus.exe"

        C:\Program Files\clusrun>(del /q /f "C:\Program Files\clusrun\uninstall.bat"   1>nul 2>&1  & exit /b 0  )
        The batch file cannot be found.
        The batch file cannot be found.
        PS C:\Program Files\clusrun> dir
        PS C:\Program Files\clusrun>
        ```
        </details>

        <details><summary>Example on Linux</summary>
    
        ```Bash
        root@ubuntu1804:~# wget https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.sh
        --2020-02-23 18:30:08--  https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.sh
        Resolving github.com (github.com)... 13.250.177.223
        Connecting to github.com (github.com)|13.250.177.223|:443... connected.
        HTTP request sent, awaiting response... 302 Found
        Location: https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/b7603280-51ca-11ea-8997-0f1ec45edf3c?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200223%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200223T183009Z&X-Amz-Expires=300&X-Amz-Signature=d94fd1fe1e52dc0e224835dc9ccc32d41962b8f86817e96ba4fbbb2f104ee82f&X-Amz-SignedHeaders=host&actor_id=0&response-content-disposition=attachment%3B%20filename%3Dsetup.sh&response-content-type=application%2Foctet-stream [following]
        --2020-02-23 18:30:09--  https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/b7603280-51ca-11ea-8997-0f1ec45edf3c?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200223%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200223T183009Z&X-Amz-Expires=300&X-Amz-Signature=d94fd1fe1e52dc0e224835dc9ccc32d41962b8f86817e96ba4fbbb2f104ee82f&X-Amz-SignedHeaders=host&actor_id=0&response-content-disposition=attachment%3B%20filename%3Dsetup.sh&response-content-type=application%2Foctet-stream
        Resolving github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)... 52.216.170.251
        Connecting to github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)|52.216.170.251|:443... connected.
        HTTP request sent, awaiting response... 200 OK
        Length: 1195 (1.2K) [application/octet-stream]
        Saving to: ‘setup.sh’

        setup.sh               100%[===========================>]   1.17K  --.-KB/s    in 0s

        2020-02-23 18:30:10 (78.1 MB/s) - ‘setup.sh’ saved [1195/1195]

        root@ubuntu1804:~# chmod +x setup.sh
        root@ubuntu1804:~# ./setup.sh
        --2020-02-23 18:30:52--  https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.tar.gz
        Resolving github.com (github.com)... 13.250.177.223
        Connecting to github.com (github.com)|13.250.177.223|:443... connected.
        HTTP request sent, awaiting response... 302 Found
        Location: https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/b798d680-4518-11ea-849d-47b5364f7588?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200223%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200223T183053Z&X-Amz-Expires=300&X-Amz-Signature=ef1966dd3d5d814bf1f342ea8c0a87bc5a0fe4907f106bf8ccd98171136d97b3&X-Amz-SignedHeaders=host&actor_id=0&response-content-disposition=attachment%3B%20filename%3Dsetup.tar.gz&response-content-type=application%2Foctet-stream [following]
        --2020-02-23 18:30:53--  https://github-production-release-asset-2e65be.s3.amazonaws.com/230954016/b798d680-4518-11ea-849d-47b5364f7588?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200223%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200223T183053Z&X-Amz-Expires=300&X-Amz-Signature=ef1966dd3d5d814bf1f342ea8c0a87bc5a0fe4907f106bf8ccd98171136d97b3&X-Amz-SignedHeaders=host&actor_id=0&response-content-disposition=attachment%3B%20filename%3Dsetup.tar.gz&response-content-type=application%2Foctet-stream
        Resolving github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)... 54.231.72.11
        Connecting to github-production-release-asset-2e65be.s3.amazonaws.com (github-production-release-asset-2e65be.s3.amazonaws.com)|54.231.72.11|:443... connected.
        HTTP request sent, awaiting response... 200 OK
        Length: 13119728 (13M) [application/octet-stream]
        Saving to: ‘clusrun.setup.tar.gz’

        clusrun.setup.tar.gz   100%[===========================>]  12.51M  4.57MB/s    in 2.7s

        2020-02-23 18:30:57 (4.57 MB/s) - ‘clusrun.setup.tar.gz’ saved [13119728/13119728]

        clus
        clusnode
        install.sh
        uninstall.sh
        Failed to stop clusrun.service: Unit clusrun.service not loaded.
        Result:
                [UBUNTU1804:50505]:     Already connected
        root@ubuntu1804:~# source /etc/profile
        root@ubuntu1804:~# clus node
        Node         State
        ----------   -----
        UBUNTU1804   Ready
        ------------------
        Node count: 1
        root@ubuntu1804:~#
        ```
        </details>

- Install, update and uninstall clusrun on Azure VMSS as a extension

    - via PowerShell

        1. Create a VMSS on Azure
        2. Optionally, create a VM in the same subnet as the headnode
        3. Install [Azure PowerShell](https://docs.microsoft.com/en-us/powershell/azure/install-az-ps?view=azps-3.4.0) module if it is not installed yet
        4. Login and select corresponding subscription in PowerShell
        5. Download and run [vmss.ps1](https://raw.githubusercontent.com/chezhang/clusrun/master/deployment/vmss.ps1) to install clusrun as a VMSS extension. Specify the node names of headnodes, to which the clusrun client should connect, when running the script, or the first instance in the VMSS will be used as a headnode

            <details><summary>Example of VMSS with Linux OS</summary>

            ```PowerShell
            PS C:\> [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            PS C:\> (New-Object System.Net.WebClient).DownloadFile("https://raw.githubusercontent.com/chezhang/clusrun/master/deployment/vmss.ps1", "$(pwd)\vmss.ps1")
            PS C:\> .\vmss.ps1 -resourceGroup chenling-vmss-1000 -vmssName vmss1 -headnodes win2019
            [02/23/2020 23:01:42] Current extensions:


            [02/23/2020 23:01:42] Adding extension Install_clusrun ...


            [02/23/2020 23:10:36] Current extensions:

            Name            StatusesSummary
            ----            ---------------
            Install_clusrun {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


            Code                        Count
            ----                        -----
            ProvisioningState/succeeded  1000

            PS C:\> .\vmss.ps1 -resourceGroup chenling-vmss-1000 -vmssName vmss1 -headnodes win2019
            [02/24/2020 00:54:14] Current extensions:

            Name            StatusesSummary
            ----            ---------------
            Install_clusrun {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


            Code                        Count
            ----                        -----
            ProvisioningState/succeeded  1000



            [02/24/2020 00:54:14] Removing extension Install_clusrun ...


            [02/24/2020 01:01:09] Current extensions:


            [02/24/2020 01:01:11] Adding extension Install_clusrun ...


            [02/24/2020 01:06:35] Current extensions:

            Name            StatusesSummary
            ----            ---------------
            Install_clusrun {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


            Code                        Count
            ----                        -----
            ProvisioningState/succeeded  1000

            PS C:\> .\vmss.ps1 -resourceGroup chenling-vmss-1000 -vmssName vmss1 -uninstall
            [02/24/2020 01:09:09] Current extensions:

            Name            StatusesSummary
            ----            ---------------
            Install_clusrun {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


            Code                        Count
            ----                        -----
            ProvisioningState/succeeded  1000



            [02/24/2020 01:09:09] Removing extension Install_clusrun ...


            [02/24/2020 01:14:33] Current extensions:


            [02/24/2020 01:14:35] Adding extension Uninstall_clusrun ...


            [02/24/2020 01:22:01] Current extensions:

            Name              StatusesSummary
            ----              ---------------
            Uninstall_clusrun {Microsoft.Azure.Management.Compute.Models.VirtualMachineStatusCodeCount}


            Code                        Count
            ----                        -----
            ProvisioningState/succeeded  1000



            [02/24/2020 01:22:04] Removing extension Uninstall_clusrun ...


            [02/24/2020 01:29:25] Current extensions:

            PS C:\>
            ```
            </details>

    - via Azure portal

        1. Create a VMSS on Azure
        2. Optionally, create a VM in the same subnet as the headnode
        3. Open `Extensions` page in `Settings` of the VMSS and click `Add`
        4. Select `Custom Script Extension` and click `Create`
        5. Download setup script from [Github](https://github.com/chezhang/clusrun/releases) (`setup.ps1` for Windows, `setup.sh` for Linux)
        6. Set `Script file` in `Install extension` page to the downloaded setup script
        7. Set `Arguments` to specify the headnode(s) (`-headnode(s) "<headnode(s)>"` for Windows, `-h "<headnodes>"` for Linux)
        8. Click `OK` to install clusrun to the VMSS as extension
        9. Upgrade VMSS instances to the latest model to apply the extension change
        10. Add `Custom Script Extension` again and set `Arguments` to update or uninstall clusrun (`-reinstall` or `-uninstall` for Windows, `-r` or `-u` for Linux)

#### Run clusrun manually

- Start clusrun on physical or virtual machine

    1. Download setup package from [Github](https://github.com/chezhang/clusrun/releases) (`setup.zip` for Windows node, `setup.tar.gz` for Linux node)
    2. Extract it to the location you want to install clusrun
    3. Start clusrun with extracted executable `clusnode` or `clusnode.exe`

- Start clusrun inside docker container

    Docker container with clusrun can be started on physical or virtual machine to join a cluster.

    - Docker container on Linux

        1. Install docker if it is not installed
        2. Download clusrun setup package `setup.tar.gz` from [Github](https://github.com/chezhang/clusrun/releases) and extract the executable `clusnode`
        3. Start docker container(s) with clusrun being hosted on available port(s) of host

        <details><summary>Example</summary>

        ```Bash
        root@ubuntu:~# docker -v
        Docker version 18.09.7, build 2d0083d
        root@ubuntu:~# wget -q https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.tar.gz
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

    - Docker container on Windows is not supported currently

## Usage examples

- Check clusrun help

    ```Bash
    clus -h
    clusnode -h
    ```

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# tail -n1 /etc/profile
    export PATH=$PATH:/usr/local/clusrun
    root@ubuntu1804:~# source /etc/profile
    root@ubuntu1804:~# clus -h

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list node(s) in the cluster
            run             - run a command or script on node(s) in the cluster
            job             - list job(s) in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h
    root@ubuntu1804:~# clusnode -h

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the clusnode
            get             - get settings of the started clusnode
            set             - set the started clusnode

    root@ubuntu1804:~# cd /usr/local/clusrun
    root@ubuntu1804:/usr/local/clusrun# ./clus

    Usage:
            clus <command> [arguments]

    The commands are:
            node            - list node(s) in the cluster
            run             - run a command or script on node(s) in the cluster
            job             - list job(s) in the cluster

    Usage of node:
            clus node [options]
            clus node -h

    Usage of run:
            clus run [options] <command>
            clus run -h

    Usage of job:
            clus job [options] [jobs]
            clus job -h
    root@ubuntu1804:/usr/local/clusrun# ./clusnode

    Usage:
            clusnode <command> [options]

    The commands are:
            start           - start the clusnode
            get             - get settings of the started clusnode
            set             - set the started clusnode

    root@ubuntu1804:/usr/local/clusrun#
    ```
    </details>

- Run command in one line

    ```Bash
    clus run apt update
    clus run apt -y install docker.io
    clus run docker run hello-world
    ```
    
    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# clus run -headnode win2019 apt update
    Job 18 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    apt update
    ------------------------------------------------------------

    [VMSS2VL2U0000JF]: WARNING:
    [VMSS2VL2U0000JF]: apt
    [VMSS2VL2U0000JF]: does not have a stable CLI interface.
    [VMSS2VL2U0000JF]: Use with caution in scripts.
    [VMSS2VL2U0000JF]: Get:1 http://security.ubuntu.com/ubuntu bionic-security InRelease [88.7 kB]
    [VMSS2VL2U0000JF]: Hit:2 http://azure.archive.ubuntu.com/ubuntu bionic InRelease
    [VMSS2VL2U0000JF]: Get:3 http://azure.archive.ubuntu.com/ubuntu bionic-updates InRelease [88.7 kB]
    [VMSS2VL2U0000JF]: Get:4 http://azure.archive.ubuntu.com/ubuntu bionic-backports InRelease [74.6 kB]
    [1/1000] Command finished on node VMSS2VL2U000009 in 2.153901423s.
    [2/1000] Command finished on node VMSS2VL2U0000IX in 2.287541912s.
    [3/1000] Command finished on node VMSS2VL2U0000G0 in 2.299897894s.
    [4/1000] Command finished on node VMSS2VL2U00001P in 2.310865167s.
    [5/1000] Command finished on node VMSS2VL2U0000O8 in 2.31742281s.
    
    ...
    
    ---------------------[VMSS2VL2U0000UJ]----------------------

    WARNING: apt does not have a stable CLI interface. Use with caution in scripts.

    Hit:1 http://azure.archive.ubuntu.com/ubuntu bionic InRelease
    Get:2 http://azure.archive.ubuntu.com/ubuntu bionic-updates InRelease [88.7 kB]
    Get:3 http://azure.archive.ubuntu.com/ubuntu bionic-backports InRelease [74.6 kB]
    Get:4 http://security.ubuntu.com/ubuntu bionic-security InRelease [88.7 kB]
    Fetched 252 kB in 1s (254 kB/s)
    Reading package lists...
    Building dependency tree...
    Reading state information...
    All packages are up to date.

    ------------------------------------------------------------
    Runtime:
    Min=2.153901423s
    Max=9.112203546s
    Mean=5.84645892s
    Mid=6.034908999s
    SD=1.424629883s

    1000 of 1000 node(s) succeeded.
    root@ubuntu1804:~#

    ...

    root@ubuntu1804:~# clus run -headnode win2019 apt -y install docker.io
    Job 23 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    apt -y install docker.io
    ------------------------------------------------------------

    [VMSS2VL2U00000A]: WARNING:
    [VMSS2VL2U00000A]: apt
    [VMSS2VL2U00000A]: does not have a stable CLI interface.
    [VMSS2VL2U00000A]: Use with caution in scripts.
    [VMSS2VL2U00000A]: Reading package lists...
    [VMSS2VL2U00000A]: Building dependency tree...
    [VMSS2VL2U00000A]: Reading state information...
    [VMSS2VL2U00000A]: The following packages were automatically installed and are no longer required:
    [VMSS2VL2U00000A]: grub-pc-bin linux-headers-4.15.0-88
    [VMSS2VL2U00000A]: Use 'apt autoremove' to remove them.
    [VMSS2VL2U00000A]: The following additional packages will be installed:
    [VMSS2VL2U00000A]: bridge-utils cgroupfs-mount containerd pigz runc ubuntu-fan
    [VMSS2VL2U00000A]: Suggested packages:
    [VMSS2VL2U00000A]: ifupdown aufs-tools debootstrap docker-doc rinse zfs-fuse | zfsutils
    [VMSS2VL2U00000A]: The following NEW packages will be installed:
    [VMSS2VL2U00000A]: bridge-utils cgroupfs-mount containerd docker.io pigz runc ubuntu-fan
    [1/1000] Command finished on node VMSS2VL2U00009B in 425.282299ms.
    [2/1000] Command finished on node VMSS2VL2U0000FJ in 434.042165ms.
    [3/1000] Command finished on node VMSS2VL2U000063 in 444.225942ms.
    [4/1000] Command finished on node VMSS2VL2U0000PI in 448.451273ms.
    [5/1000] Command finished on node VMSS2VL2U0000T6 in 448.994978ms.
    [VMSS2VL2U00000A]: 0 upgraded, 7 newly installed, 0 to remove and 0 not upgraded.
    Need to get 129 kB/52.2 MB of archives.
    After this operation, 257 MB of additional disk space will be used.
    Get:1 http://azure.archive.ubuntu.com/ubuntu bionic/universe amd64 pigz amd64 2.4-1 [57.4 kB]
    [6/1000] Command finished on node VMSS2VL2U0000TF in 453.154109ms.
    [7/1000] Command finished on node VMSS2VL2U0000J5 in 453.453011ms.
    [VMSS2VL2U00000A]: Get:2 http://azure.archive.ubuntu.com/ubuntu bionic/main amd64 bridge-utils amd64 1.5-15ubuntu1 [30.1 kB]
    [8/1000] Command finished on node VMSS2VL2U0000LY in 457.015238ms.
    [VMSS2VL2U00000A]: Get:3 http://azure.archive.ubuntu.com/ubuntu bionic/universe amd64 cgroupfs-mount all 1.4 [6320 B]
    [VMSS2VL2U00000A]: Get:4 http://azure.archive.ubuntu.com/ubuntu bionic/main amd64 ubuntu-fan all 0.12.10 [34.7 kB]
    [9/1000] Command finished on node VMSS2VL2U000041 in 477.016588ms.
    [10/1000] Command finished on node VMSS2VL2U00008G in 477.135489ms.

    ...

    ---------------------[VMSS2VL2U0000UJ]----------------------

    WARNING: apt does not have a stable CLI interface. Use with caution in scripts.

    Reading package lists...
    Building dependency tree...
    Reading state information...
    docker.io is already the newest version (18.09.7-0ubuntu1~18.04.4).
    The following packages were automatically installed and are no longer required:
    grub-pc-bin linux-headers-4.15.0-88
    Use 'apt autoremove' to remove them.
    0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.

    ------------------------------------------------------------
    Runtime:
    Min=425.282299ms
    Max=1m35.880204409s
    Mean=19.118817518s
    Mid=2.344294984s
    SD=18.990610711s

    998 of 1000 node(s) succeeded.
    Failed node(s) (2/1000): VMSS2VL2U0000FM, VMSS2VL2U0000GE
    root@ubuntu1804:~# clus run -headnode win2019 apt -y install docker.io
    Job 24 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    apt -y install docker.io
    ------------------------------------------------------------

    [VMSS2VL2U000075]: WARNING:
    [VMSS2VL2U000075]: apt
    [VMSS2VL2U000075]: does not have a stable CLI interface.
    [VMSS2VL2U000075]: Use with caution in scripts.
    [VMSS2VL2U000075]: Reading package lists...
    [VMSS2VL2U000075]: Building dependency tree...
    [VMSS2VL2U000075]: Reading state information...
    [1/1000] Command finished on node VMSS2VL2U0000KK in 449.374088ms.
    [2/1000] Command finished on node VMSS2VL2U00009B in 451.100101ms.
    [3/1000] Command finished on node VMSS2VL2U0000MA in 451.153401ms.
    [4/1000] Command finished on node VMSS2VL2U000066 in 451.176201ms.
    [5/1000] Command finished on node VMSS2VL2U0000U5 in 451.211102ms.
    [6/1000] Command finished on node VMSS2VL2U0000A2 in 452.34261ms.
    [7/1000] Command finished on node VMSS2VL2U00004L in 452.35801ms.
    [8/1000] Command finished on node VMSS2VL2U0000PI in 452.36331ms.
    [9/1000] Command finished on node VMSS2VL2U0000HY in 452.427411ms.
    [10/1000] Command finished on node VMSS2VL2U0000U9 in 452.445811ms.

    ...

    ---------------------[VMSS2VL2U0000UJ]----------------------

    WARNING: apt does not have a stable CLI interface. Use with caution in scripts.

    Reading package lists...
    Building dependency tree...
    Reading state information...
    docker.io is already the newest version (18.09.7-0ubuntu1~18.04.4).
    The following packages were automatically installed and are no longer required:
    grub-pc-bin linux-headers-4.15.0-88
    Use 'apt autoremove' to remove them.
    0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.

    ------------------------------------------------------------
    Runtime:
    Min=449.374088ms
    Max=35.583674732s
    Mean=1.555574319s
    Mid=1.561995374s
    SD=1.687802221s

    1000 of 1000 node(s) succeeded.

    ...

    root@ubuntu1804:~# clus run -headnode win2019 docker run hello-world
    Job 26 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    docker run hello-world
    ------------------------------------------------------------

    [VMSS2VL2U00008W]: Hello from Docker!
    This message shows that your installation appears to be working correctly.

    To generate this message, Docker took the following steps:
    1. The Docker client contacted the Docker daemon.
    2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
        (amd64)
    3. The Docker daemon created a new container from that image which runs the
        executable that produces the output you are currently reading.
    4. The Docker daemon streamed that output to the Docker client, which sent it
        to your terminal.

    To try something more ambitious, you can run an Ubuntu container with:
    $ docker run -it ubuntu bash

    Share images, automate workflows, and more with a free Docker ID:
    https://hub.docker.com/

    For more examples and ideas, visit:
    https://docs.docker.com/get-started/
    [1/1000] Command finished on node VMSS2VL2U00003U in 1.5779009s.
    [2/1000] Command finished on node VMSS2VL2U0000R9 in 1.58189493s.
    [3/1000] Command finished on node VMSS2VL2U0000G7 in 1.600298669s.
    [4/1000] Command finished on node VMSS2VL2U0000DL in 1.608760233s.
    [5/1000] Command finished on node VMSS2VL2U0000JM in 1.613043265s.
    [6/1000] Command finished on node VMSS2VL2U0000K7 in 1.615782586s.

    ...

    ---------------------[VMSS2VL2U0000UJ]----------------------

    Hello from Docker!
    This message shows that your installation appears to be working correctly.

    To generate this message, Docker took the following steps:
    1. The Docker client contacted the Docker daemon.
    2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
        (amd64)
    3. The Docker daemon created a new container from that image which runs the
        executable that produces the output you are currently reading.
    4. The Docker daemon streamed that output to the Docker client, which sent it
        to your terminal.

    To try something more ambitious, you can run an Ubuntu container with:
    $ docker run -it ubuntu bash

    Share images, automate workflows, and more with a free Docker ID:
    https://hub.docker.com/

    For more examples and ideas, visit:
    https://docs.docker.com/get-started/


    ------------------------------------------------------------
    Runtime:
    Min=1.5779009s
    Max=4.440492689s
    Mean=2.28628119s
    Mid=2.237594825s
    SD=503.753424ms

    1000 of 1000 node(s) succeeded.
    root@ubuntu1804:~#
    ```
    </details>

- Run commands in script

    ```Bash
    echo apt update > helloworld
    echo apt -y install docker.io >> helloworld
    echo docker run hello-world >> helloworld
    clus run -script helloworld
    ```

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# echo apt update > helloworld
    root@ubuntu1804:~# echo apt -y install docker.io >> helloworld
    root@ubuntu1804:~# echo docker run hello-world >> helloworld
    root@ubuntu1804:~# clus run -headnode win2019 -script helloworld
    Job 29 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    apt update
    apt -y install docker.io
    docker run hello-world

    ------------------------------------------------------------

    [VMSS2VL2U00006G]: WARNING:
    [VMSS2VL2U00006G]: apt
    [VMSS2VL2U00006G]: does not have a stable CLI interface.
    [VMSS2VL2U00006G]: Use with caution in scripts.
    [VMSS2VL2U00006G]: Hit:1 http://azure.archive.ubuntu.com/ubuntu bionic InRelease
    Get:2 http://azure.archive.ubuntu.com/ubuntu bionic-updates InRelease [88.7 kB]
    Get:3 http://azure.archive.ubuntu.com/ubuntu bionic-backports InRelease [74.6 kB]
    [VMSS2VL2U00006G]: Get:4 http://security.ubuntu.com/ubuntu bionic-security InRelease [88.7 kB]
    [VMSS2VL2U00006G]: Fetched 252 kB in 1s (216 kB/s)
    Reading package lists...
    [VMSS2VL2U00006G]: Building dependency tree...
    [VMSS2VL2U00006G]: Reading state information...
    [VMSS2VL2U00006G]: All packages are up to date.
    [VMSS2VL2U00006G]: WARNING:
    [VMSS2VL2U00006G]: apt
    [VMSS2VL2U00006G]: does not have a stable CLI interface.
    [VMSS2VL2U00006G]: Use with caution in scripts.
    [VMSS2VL2U00006G]: Reading package lists...
    [VMSS2VL2U00006G]: Building dependency tree...
    [VMSS2VL2U00006G]: Reading state information...
    [VMSS2VL2U00006G]: docker.io is already the newest version (18.09.7-0ubuntu1~18.04.4).
    The following package was automatically installed and is no longer required:
    [VMSS2VL2U00006G]: grub-pc-bin
    [VMSS2VL2U00006G]: Use 'apt autoremove' to remove it.
    [VMSS2VL2U00006G]: 0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.
    [1/1000] Command finished on node VMSS2VL2U00005J in 9.276423797s.
    [2/1000] Command finished on node VMSS2VL2U0000MB in 9.373765378s.
    [3/1000] Command finished on node VMSS2VL2U0000QZ in 9.465244205s.
    [4/1000] Command finished on node VMSS2VL2U0000J1 in 10.329063316s.

    ...

    ---------------------[VMSS2VL2U0000UJ]----------------------
    (Truncated)
    ...4).
    The following package was automatically installed and is no longer required:
    grub-pc-bin
    Use 'apt autoremove' to remove it.
    0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.

    Hello from Docker!
    This message shows that your installation appears to be working correctly.

    To generate this message, Docker took the following steps:
    1. The Docker client contacted the Docker daemon.
    2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
        (amd64)
    3. The Docker daemon created a new container from that image which runs the
        executable that produces the output you are currently reading.
    4. The Docker daemon streamed that output to the Docker client, which sent it
        to your terminal.

    To try something more ambitious, you can run an Ubuntu container with:
    $ docker run -it ubuntu bash

    Share images, automate workflows, and more with a free Docker ID:
    https://hub.docker.com/

    For more examples and ideas, visit:
    https://docs.docker.com/get-started/


    ------------------------------------------------------------
    Runtime:
    Min=9.276423797s
    Max=19.696080206s
    Mean=16.53068604s
    Mid=16.610838066s
    SD=1.023832691s

    1000 of 1000 node(s) succeeded.
    root@ubuntu1804:~#
    ```
    </details>

- Dump output to file

    ```Bash
    clus run -dump docker run hello-world
    ll | grep clus.run
    ```

    <details><summary>Example</summary>

    ```Bash
    root@ubuntu1804:~# ulimit -n 65536
    root@ubuntu1804:~# clus run -headnode win2019 -dump docker run hello-world
    Job 31 started on 1000 node(s) in cluster win2019:50505.

    --------------------------Command---------------------------
    docker run hello-world
    ------------------------------------------------------------

    [VMSS2VL2U0000LD]: Hello from Docker!
    This message shows that your installation appears to be working correctly.

    To generate this message, Docker took the following steps:
    1. The Docker client contacted the Docker daemon.
    2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
        (amd64)
    3. The Docker daemon created a new container from that image which runs the
        executable that produces the output you are currently reading.
    4. The Docker daemon streamed that output to the Docker client, which sent it
        to your terminal.

    To try something more ambitious, you can run an Ubuntu container with:
    $ docker run -it ubuntu bash

    Share images, automate workflows, and more with a free Docker ID:
    https://hub.docker.com/

    For more examples and ideas, visit:
    https://docs.docker.com/get-started/
    [1/1000] Command finished on node VMSS2VL2U00006H in 1.539255642s.
    [2/1000] Command finished on node VMSS2VL2U0000DL in 1.555419075s.
    [3/1000] Command finished on node VMSS2VL2U00004L in 1.578160161s.
    [4/1000] Command finished on node VMSS2VL2U0000I1 in 1.588761748s.
    [5/1000] Command finished on node VMSS2VL2U0000PP in 1.597786523s.
    [6/1000] Command finished on node VMSS2VL2U0000E7 in 1.598350327s.

    ...

    ---------------------[VMSS2VL2U0000UJ]----------------------

    Hello from Docker!
    This message shows that your installation appears to be working correctly.

    To generate this message, Docker took the following steps:
    1. The Docker client contacted the Docker daemon.
    2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
        (amd64)
    3. The Docker daemon created a new container from that image which runs the
        executable that produces the output you are currently reading.
    4. The Docker daemon streamed that output to the Docker client, which sent it
        to your terminal.

    To try something more ambitious, you can run an Ubuntu container with:
    $ docker run -it ubuntu bash

    Share images, automate workflows, and more with a free Docker ID:
    https://hub.docker.com/

    For more examples and ideas, visit:
    https://docs.docker.com/get-started/


    ------------------------------------------------------------
    Runtime:
    Min=1.539255642s
    Max=6.554736521s
    Mean=2.709754903s
    Mid=3.214486548s
    SD=787.992201ms

    1000 of 1000 node(s) succeeded.
    root@ubuntu1804:~# ll | grep clus.run
    drw-r--r--  2 root root    86016 Feb 24 06:49 clus.run.20200224064936/
    root@ubuntu1804:~# ls clus.run.20200224064936 | wc -l
    2000
    root@ubuntu1804:~# 
    ```
    </details>

## Issues and solutions

### ARP cache limit

#### Environment

- Azure VMSS with 1000 instances as cluster nodes

- Ubuntu1804 as cluster headnode and client

#### Symptom

- Headnode can not be connected locally by `clus`

    ```Bash
    root@ubuntu1804:~# clus node
    Can not connect: context deadline exceeded
    Please ensure the headnode localhost:50505 is started and accessible
    ```

- Headnode can be connected from other node

    ```PowerShell
    PS C:\Program Files\clusrun> .\clus.exe node -headnode ubuntu1804
    Node              State
    ---------------   -----
    ...               ...
    -----------------------
    Node count: 1001
    ```

- Failure to ping localhost or other node from headnode

    ```Bash
    root@ubuntu1804:~# ping localhost
    ping: sendmsg: Invalid argument
    ping: sendmsg: Invalid argument
    ping: sendmsg: Invalid argument
    ...
    ```

#### Solution

Increase ARP cache limit, referenced to [Huang Huang 的博客](https://mozillazg.com/2017/10/linux-a-way-to-fix-haproxy-network-connection-timeout-ping-sendmsg-invalid-argument-socket-errno-110-connection-timed-out)

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

### Open files limit

#### Environment

- Azure VMSS with 1000 instances as cluster nodes

- Ubuntu1804 as client

#### Symptom

- Client fails to dump output files with error `too many open files` 

    ```Bash
    Failed to create output file: open /root/clus.run.20200224031342/VMSS2VL2U00004T.err: too many open files
    ```

#### Solution

Increase open files limit

```Bash
root@ubuntu1804:~# ulimit -n
1024
root@ubuntu1804:~# ulimit -n 65536
root@ubuntu1804:~# clus run -headnode win2019 -dump apt update
```