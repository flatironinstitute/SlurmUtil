#!/usr/bin/bash

module add slurm gcc/10.1.0 python3
cd /mnt/home/yliu/projects/slurm/utils/
. env_slurm18_python37/bin/activate
#expect script.exp, use ~/.my.cnf instead
./mysqldump.sh
#generate csv and default forecast image files, default influx history
python daily.py
/cm/shared/apps/slurm/18.08.8/bin/sacctmgr list user -P -s >& /mnt/home/yliu/projects/slurm/utils/data/sacctmgr_assoc.csv
