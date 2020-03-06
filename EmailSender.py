import smtplib
from datetime import timedelta
from collections import defaultdict as DDict
import MyTool

# Import the email modules we'll need
from email.message import EmailMessage

RECIPIENTS    =['yliu@flatironinstitute.org','ncarriero@flatironinstitute.org','dsimon@flatironinstitute.org']
MSG_LOW_UTIL  ='Dear {}, \n\nYour job {} has run for {} with an average CPU utilization of {:.2f} and MEM utilization of {:.2f} on node {} with a total of {} cpus. You may check the details of the resource usage at {}. \n\n Please verify that this job is behaving as expected. If you no longer need the job, please terminate it (e.g., use "scancel"), so that the resources allocated to it can be used by others. \n\n Thank you very much! \n SCC team'
SCC_USERS     =['yliu', 'ncarriero', 'dsimon', 'ifisk', 'apataki', 'jcreveling', 'awatter', 'ntrikoupis', 'jmoore', 'pgunn', 'achavkin', 'elovero', 'rblackwell']


def getMsg_test ():
    content='test1\n\ntest2'

    msg = EmailMessage()
    msg.set_content(content)
    msg['Subject'] = 'test1'
    msg['From']    = 'yliu'
    msg['To']      = 'yliu@flatironinstitute.org'

    return msg

class JobNoticeSender:
    def __init__(self, interval=86400, cacheFile='jobNotice.cache'):
        self.cacheFile       = cacheFile
        self.last_jobNotice  = MyTool.readFile(cacheFile)               #{jid:ts}
        if not self.last_jobNotice:
           self.last_jobNotice = DDict(int)
        self.interval_secs   = interval         #interval to send repeated notice for the same job

    def sendNotice (self, ts, jobs):
        curr_jids      = set(jobs.keys())
        last_jids      = set(self.last_jobNotice.keys())
        
        dup_jids  = curr_jids.intersection(last_jids)
        rmv_jids  = last_jids.difference(dup_jids)
        new_jids  = curr_jids.difference(dup_jids)

        #remove the job notice that is not repeated this time, if job's utilization is fluctrated around border-line->more than expected dup notices
        for jid in rmv_jids:
            self.last_jobNotice.pop(jid)
        #send notice for new appeared job
        for jid in new_jids:
            self.sendJobNotice (ts, jobs[jid])
            self.last_jobNotice[jid] = ts
        #send notice for dup job if long interval passed
        dup_jids_send = list(filter(lambda x: ts - self.last_jobNotice[x] > self.interval_secs, dup_jids))
        #print('{}: send notice for {} conditionly ({})'.format(ts, dup_jids_send, dup_jids))
        for jid in dup_jids_send:
            self.sendJobNotice(ts, jobs[jid])
            self.last_jobNotice[jid] = ts

        MyTool.writeFile (self.cacheFile, self.last_jobNotice)
        
    def sendJobNotice (self, ts, job):
        user   =MyTool.getUserStruct(int(job['user_id']))
        groups =MyTool.getUserGroups(user.pw_name)
        if 'scc' in groups:
           return
        
        userName     = user.pw_name
        userFullName = user.pw_gecos.split(',')[0]
        addr   ='http://mon7:8126/jobDetails?jid={}'.format(job['job_id'])
        content=MSG_LOW_UTIL.format(userFullName, job['job_id'], timedelta(seconds=ts - int(job['start_time'])), job['job_avg_util'], job['job_mem_util'], job['nodes'], job['num_cpus'], addr)
        #to_list=RECIPIENTS + ['@flatironinstitute.org'.format(userName)]
        to_list=RECIPIENTS

        msg = EmailMessage()
        msg.set_content(content)
        msg['Subject'] = 'Long runnig job with low utilization at slurm cluster -- Job {} by {}'.format(job['job_id'], userName)
        msg['From']    = 'yliu'
        msg['To']      = ', '.join(to_list)
        
        with smtplib.SMTP('localhost') as s:
            s.send_message(msg)
          
def main():
    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost')
    msg = getMsg_test()
    print('{}'.format(msg))
    s.send_message(msg)
    s.quit()

if __name__=="__main__":
   main()
