#!/usr/bin/bash

module add slurm gcc/10.1.0 python3
LOG=daily_$(date +%Y-%m-%d).log
clusters=`sacctmgr -nP list cluster format=Cluster`
cd /mnt/home/yliu/projects/slurm/utils/
. env_slurm18_python37/bin/activate
pwd >& ${LOG}
#expect script.exp, use ~/.my.cnf instead
./mysqldump.sh $clusters >> ${LOG}
#generate csv and default forecast image files, default influx history
python daily.py -c $clusters >> ${LOG}
#python prophet.py -c $clusters -u -y 2
/cm/shared/apps/slurm/current/bin/sacctmgr list user -P -s >& /mnt/home/yliu/projects/slurm/utils/data/sacctmgr_assoc.csv
echo "DONE" >> ${LOG}
deactivate
