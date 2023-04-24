#!/usr/bin/bash

#module not work in crontab, the module shell function is loaded by /etc/profile.d/modules.sh which won't be loaded by a bare /bin/sh, only by an interactive bash (/etc/bashrc).
. /etc/profile.d/modules.sh
module add slurm gcc/10 python3

LOG=daily_$(date +%Y-%m-%d_%H).log   #the same name as log file used by daily.py

cd /mnt/home/yliu/projects/slurm/utils/
. env_slurm22_p310/bin/activate

#clusters=`/cm/shared/apps/slurm/current/bin/sacctmgr -nP list cluster format=Cluster`
#slurm_cluster is replaced by slurm
clusters='slurm'
pwd >& ${LOG}

#expect script.exp, use ~/.my.cnf instead
#dump slurmdb data to /mnt/home/yliu/projects/slurm/utils/data
./mysqldump.sh $clusters >> ${LOG} 2>&1

clusters='slurm'
#save old prophet files
#generate csv and default forecast image files, default influx history
python daily.py -c $clusters >> ${LOG} 2>&1

/cm/shared/apps/slurm/current/bin/sacctmgr list user -P -s >& ./data/sacctmgr_assoc.csv
ssh -i /mnt/home/yliu/.ssh/id_sdsc popeye.sdsc.edu "module add slurm;sacctmgr -P -s list user" >& ./data/sacctmgr_assoc_popeye.csv

#get users.csv from scc-ansible github repo
#once the file is changed, the address is changed
cd ~yliu/projects/scc-ansible
#git fetch origin
#diff=`git diff users.csv`
#diff1=`git diff sdsc.csv`
#if [ ! -z "$diff" ] || [! -z "$diff1"] 
#then
   git pull 
   cp users.csv ~yliu/projects/slurm/utils/data/
   cp sdsc.csv ~yliu/projects/slurm/utils/data/
#fi
#wget -O ./data/users.csv https://raw.githubusercontent.com/flatironinstitute/scc-ansible/master/users.csv?token=AFMF3P7DKKTLWF7NTRJ2TYLAX6AFY

echo "DONE" >> ${LOG}
deactivate
