datestr=$(date +%Y_%m)
clusters=$@
tables="usage_hour_table assoc_usage_hour_table usage_day_table assoc_usage_day_table assoc_table job_table step_table"
tgt_dir=/mnt/home/yliu/projects/slurm/utils/data
back_dir=${tgt_dir}/${datestr}/
[ -d ${back_dir} ] || mkdir ${back_dir}

echo "$0: Clusters are ${clusters}"
cd ~/projects/slurm/utils
table="qos_table"
echo "${table}..."
mv ${tgt_dir}/${table}.csv ${back_dir}/
mysqldump slurm_acct_db ${table} >& mysqldump-to-csv/${table}_${datestr}.txt
python mysqldump_to_csv.py mysqldump-to-csv/${table}_${datestr}.txt >& ${tgt_dir}/${table}.csv
for cluster in ${clusters}
do
   for tbl in ${tables}
   do
      table=${cluster}_${tbl}
      echo "${table}..."
      mv ${tgt_dir}/${table}.csv ${back_dir}/
      mysqldump slurm_acct_db ${table} >& mysqldump-to-csv/${table}_${datestr}.txt
      python mysqldump_to_csv.py mysqldump-to-csv/${table}_${datestr}.txt >& ${tgt_dir}/${table}.csv
   done
done
#put information in ~/.my.cnf
#mysqldump -h ironbcm1 -u slurmreadonly -p slurm_acct_db ${table1} >& mysqldump-to-csv/${table1}_${datestr}.txt
#mysql -u slurmreadonly -p slurm_acct_db

