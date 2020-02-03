#!/bin/bash

[[ $EUID -ne 0 ]] && echo root privilege is required && exit

headnodes="localhost"
location="/usr/local/clusrun"
reinstall=false
uninstall=false
while getopts h:l:ru option; do
    case "${option}" in
        h) headnodes=${OPTARG};;
        l) location=${OPTARG};;
        r) reinstall=true;;
        u) uninstall=true;;
    esac
done

setup_url="https://github.com/chezhang/clusrun/releases/download/0.1.0/setup.tar.gz"
setup_file="clusrun.setup.tar.gz"

if $uninstall || $reinstall; then
    cd $location
    ./uninstall.sh
    if $uninstall; then
        exit
    fi
fi

for i in {1..10}; do
    wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 0 --progress=bar:force -O $setup_file $setup_url
    [ $? -eq 0 ] && break || sleep 1
done
mkdir -p $location
tar xzvf $setup_file -C $location
rm $setup_file

cd $location
./install.sh
rm install.sh
sleep 1
./clusnode set -headnodes "$headnodes"

add_to_path="export PATH=\$PATH:$location"
grep -Fxq "$add_to_path" /etc/profile || echo $add_to_path >>/etc/profile
