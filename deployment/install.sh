#!/bin/bash

systemctl stop clusrun

cat <<EOF >/etc/systemd/system/clusrun.service
[Unit]
Description=clusrun service

[Service]
User=root
ExecStart=$(pwd)/clusnode start

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl start clusrun