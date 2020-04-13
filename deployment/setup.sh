#!/bin/bash

[[ $EUID -ne 0 ]] && echo root privilege is required && exit

headnodes="localhost"
port=50505
location="/usr/local/clusrun"
setup_url="https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.tar.gz"
reinstall=false
uninstall=false
while getopts h:l:s:ru option; do
    case "${option}" in
        h) headnodes=${OPTARG};;
        p) port=${OPTARG};;
        l) location=${OPTARG};;
        s) setup_url=${OPTARG};;
        r) reinstall=true;;
        u) uninstall=true;;
    esac
done

if $uninstall || $reinstall; then
    bash "$location/uninstall.sh"
    if $uninstall; then
        exit
    fi
fi

shopt -s nocasematch
if [[ $setup_url == http* ]]; then
    setup_file="clusrun.setup.tar.gz"
    for i in {1..10}; do
        wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 0 --progress=bar:force -O $setup_file $setup_url
        [ $? -eq 0 ] && break || sleep 1
    done
    setup_url=$setup_file
fi
shopt -u nocasematch

mkdir -p $location
tar xzvf $setup_url -C $location

cd $location
./install.sh $port
rm install.sh
sleep 1
./clusnode config set -headnodes "$headnodes" -node "localhost:$port"

add_to_path="export PATH=\$PATH:$location"
grep -Fxq "$add_to_path" /etc/profile || echo $add_to_path >>/etc/profile

echo
echo Clusrun is installed in $location
echo Please source /etc/profile or start a new login shell to load updated PATH
echo
