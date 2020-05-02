#! /bin/bash

sudo sysctl -w kern.maxfiles=20480
sudo sysctl -w kern.maxfilesperproc=30000
ulimit -n 20000

exec ghcid \
     --test 'DevelMain.update' -W --color=always \
     -c 'stack ghci odd-jobs:exe:devel' \
     --reverse-errors --no-height-limit \
     --restart stack.yaml \
     --restart package.yaml \
     --allow-eval \
     --clear
