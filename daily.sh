#!/usr/bin/bash

cd /mnt/home/yliu/projects/slurm/utils/
. env_slurm18/bin/activate
expect script.exp
python prophet.py
python queryInflux.py
/cm/shared/apps/slurm/18.08.8/bin/sacctmgr list user -P -s >& sacctmgr_assoc.csv
