#!/bin/bash

[[ $EUID -ne 0 ]] && echo root privilege is required && exit

headnodes="localhost"
port=50505
location="/usr/local/clusrun"
setup_url="https://github.com/chezhang/clusrun/releases/download/v0.2.0/setup.tar.gz"
reinstall=false
uninstall=false
cert_file=""
key_file=""
cert_base64=""
key_base64=""
while getopts h:l:s:ruc:k:e:y: option; do
    case "${option}" in
        h) headnodes=${OPTARG};;
        p) port=${OPTARG};;
        l) location=${OPTARG};;
        s) setup_url=${OPTARG};;
        r) reinstall=true;;
        u) uninstall=true;;
        c) cert_file=${OPTARG};;
        k) key_file=${OPTARG};;
        e) cert_base64=${OPTARG};;
        y) key_base64=${OPTARG};;
    esac
done

if !$uninstall; then
    [ -z "$cert_file" ] && [ -z $cert_base64 ] && echo "Please specify the cert for secure communication." && exit
    [ -z "$key_file" ] && [ -z $key_base64 ] && echo "Please specify the key for secure communication." && exit
fi

if $uninstall || $reinstall; then
    bash "$location/uninstall.sh" -cleanup
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

cert="$location/cert.pem"
key="$location/key.pem"

[ ! -z $cert_base64 ] && echo "Create cert file $cert from base64" && echo $cert_base64 | base64 -d >$cert
[ ! -z $key_base64 ] && echo "Create key file $key from base64" && echo $key_base64 | base64 -d >$key
[ ! -z "$cert_file" ] && echo "Create cert file $cert from file $cert_file" && cat $cert_file >$cert
[ ! -z "$key_file" ] && echo "Create key file $key from file $key_file" && cat $key_file >$key

cd $location
./install.sh $port
rm install.sh
sleep 1
./clusnode config set -headnodes "$headnodes" -node "localhost:$port"

echo
echo Clusrun is installed in $location
echo
