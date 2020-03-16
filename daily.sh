#!/usr/bin/bash

cd /mnt/home/yliu/projects/slurm/utils/
expect script.exp
python prophet.py
/cm/shared/apps/slurm/18.08.8/bin/sacctmgr list user -P -s >& sacctmgr_assoc.csv
