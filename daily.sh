#!/usr/bin/bash

#module not work in crontab, the module shell function is loaded by /etc/profile.d/modules.sh which won't be loaded by a bare /bin/sh, only by an interactive bash (/etc/bashrc).
#module add slurm gcc/10.1.0 python3
LOG=daily_$(date +%Y-%m-%d).log
clusters=`/cm/shared/apps/slurm/current/bin/sacctmgr -nP list cluster format=Cluster`
cd /mnt/home/yliu/projects/slurm/utils/
. env_slurm18_python37/bin/activate
pwd >& ${LOG}
#expect script.exp, use ~/.my.cnf instead
./mysqldump.sh $clusters >> ${LOG} 2>&1
#generate csv and default forecast image files, default influx history
#start python environment?
python daily.py -c $clusters >> ${LOG} 2>&1
python prophet.py -c $clusters -u -y 2 >> ${LOG} 2>&1
/cm/shared/apps/slurm/current/bin/sacctmgr list user -P -s >& /mnt/home/yliu/projects/slurm/utils/data/sacctmgr_assoc.csv
echo "DONE" >> ${LOG}
deactivate
