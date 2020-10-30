datestr=$(date +%Y_%m)
tgt_dir=/mnt/home/yliu/projects/slurm/utils/data
tables="slurm_cluster_usage_hour_table slurm_cluster_assoc_usage_hour_table slurm_cluster_assoc_usage_day_table slurm_cluster_assoc_table slurm_cluster_job_table qos_table"

cd ~/projects/slurm/utils
for table in ${tables}
do
    echo "${table}..."
    mysqldump slurm_acct_db ${table} >& mysqldump-to-csv/${table}_${datestr}.txt
    python mysqldump_to_csv.py mysqldump-to-csv/${table}_${datestr}.txt >& ${tgt_dir}/${table}.csv
done
#put information in ~/.my.cnf
#mysqldump -h ironbcm1 -u slurmreadonly -p slurm_acct_db ${table1} >& mysqldump-to-csv/${table1}_${datestr}.txt
#mysql -u slurmreadonly -p slurm_acct_db
