import smtplib
from datetime import timedelta
from collections import defaultdict as DDict
import MyTool

# Import the email modules we'll need
from email.message import EmailMessage

RECIPIENTS    =['yliu@flatironinstitute.org']
#RECIPIENTS    =['yliu@flatironinstitute.org', 'scicomp@flatironinstitute.org']
#RECIPIENTS    =['yliu@flatironinstitute.org','ncarriero@flatironinstitute.org','dsimon@flatironinstitute.org']
MSG_LOW_UTIL  ='Dear {}, \n\nYour job {} has run for {} with an average CPU utilization of {:.2f} and MEM utilization of {:.2f} on node {} with a total of {} cpus. You may check the details of the resource usage at {}. \n\n Please verify that this job is behaving as expected. If you no longer need the job, please terminate it (e.g., use "scancel"), so that the resources allocated to it can be used by others. \n\n Thank you very much! \n SCC team'
SUMMARY_LOW_UTIL = """Greetings,

Please find below a compilation of jobs that demonstrate minimal resource usage, with an average CPU utilization below 0.{:02d} (current CPU utilization also being below 0.{:02d}), MEM utilization below 0.{:02d}, and an average GPU utilization (if applicable) that has been below 0.{:02d} within the last hour. Further details are available at http://mon7:8126/bulletinboard.

{}

Warm regards."""

def sendMessage (subject, content, to='yliu@flatironinstitute.org', sender='SlurmMonitor@flatironinstitute.org'):
    print("sendMessage")
    msg = EmailMessage()
    msg.set_content(content)
    msg['Subject'] = subject
    msg['From']    = sender
    msg['To']      = to
    with smtplib.SMTP('smtp-relay.gmail.com') as s:
         s.send_message(msg)

class JobNoticeSender:
    def __init__(self, interval=86400, cacheFile='jobNotice.cache'):
        self.cacheFile       = cacheFile                                #disk file to persist self.sav_jobNotice
        self.sav_jobNotice  = MyTool.readFile(cacheFile)               #{jid:sent_ts}
        if not self.sav_jobNotice:
           self.sav_jobNotice = DDict(int)
        self.interval_secs   = interval         #interval to send repeated notice for the same job, 86400 sec=24 hours

    # remove job with sent_ts too old 
    def expireSavedNotice (self, ts):
        newDict = dict(filter(lambda elem: ts - elem[1] < self.interval_secs, self.sav_jobNotice.items()))
        self.sav_jobNotice = newDict

    def sendNotice (self, ts, jobs):
        self.expireSavedNotice (ts)

        for jid in jobs.keys():
            if jid not in self.sav_jobNotice:                 #not sent in save window
               self.sendJobNotice (ts, jobs[jid])
               self.sav_jobNotice[jid] = ts

        MyTool.writeFile (self.cacheFile, self.sav_jobNotice)
        
    def sendJobNotice (self, ts, job):
        user         = MyTool.getUserStruct(int(job['user_id']))
        userName     = user.pw_name
        userFullName = user.pw_gecos.split(',')[0]
        addr   ='http://mon7:8126/jobDetails?jid={}'.format(job['job_id'])
        content=MSG_LOW_UTIL.format(userFullName, job['job_id'], timedelta(seconds=ts - int(job['start_time'])), job['job_avg_util'], job['job_mem_util'], job['nodes'], job['num_cpus'], addr)
        #to_list=RECIPIENTS + ['@flatironinstitute.org'.format(userName)]
        to_list=RECIPIENTS

        print("sendMessage {}".format(content))
        sendMessage('[scicomp] Job {} with low utilization'.format(job['job_id'], userName), content, to=','.join(to_list))
          
    # send low utilization summary
    def sendLUSummary (self, ts, jobs, lmt_settings):
        content      = SUMMARY_LOW_UTIL.format(lmt_settings['cpu'], lmt_settings['cpu'], lmt_settings['mem'], lmt_settings['gpu'], list(jobs.keys()))
        print ("{}".format(content))
        to_list      = RECIPIENTS


        print("sendMessage {}".format(content))
        sendMessage('[scicomp] Slurm Jobs of minimal resource usage', content, ','.join(to_list))
        print ("Done sendLUSummary")


def main():
    tst = JobNoticeSender()
    tst.sendLUSummary(9843750, {'a':1, 'b':2}, {'cpu':10, 'mem':10, 'gpu':10})
    sendMessage('test', 'test', to='yliu@flatironinstitute.org')#not reaching outside email

if __name__=="__main__":
   main()
