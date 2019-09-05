import smtplib
from datetime import timedelta
import MyTool

# Import the email modules we'll need
from email.message import EmailMessage

RECIPIENTS    =['yliu@flatironinstitute.org','ncarriero@flatironinstitute.org','dsimon@flatironinstitute.org']
#RECIPIENTS    =['yliu@flatironinstitute.org','yanbin_liu@yahoo.com']
MSG_LOW_UTIL  ="Dear user, \n\nYour job {} has been run for {} with a average utilization of {:.2f} on nodes {}. You may check the details of the resource usage at {}. \n\n Please revise your job to see if you need the job to continue. \n\n Thank you very much! \n SCC team"

def getMsg_test ():
    content='test1\n\ntest2'

    msg = EmailMessage()
    msg.set_content(content)
    msg['Subject'] = 'test1'
    msg['From']    = 'yliu'
    msg['To']      = 'yliu@flatironinstitute.org'

    return msg

class JobNoticeSender:
    def __init__(self, interval=86400):
        self.last_jobNotice  = {}               #{jid:ts}
        self.interval_secs   = interval         #interval to send repeated notice for the same job

    def sendNotice (self, ts, jobs):
        curr_jids      = set(jobs.keys())
        last_jids      = set(self.last_jobNotice.keys())
        
        dup_jids  = curr_jids.intersection(last_jids)
        rmv_jids  = last_jids.difference(dup_jids)
        new_jids  = curr_jids.difference(dup_jids)

        #remove the job notice that is not repeated this time, if job's utilization is fluctrated around border-line->more than expected dup notices
        #print('remove {}'.format(rmv_jids))
        for jid in rmv_jids:
            self.last_jobNotice.pop(jid)
        #send notice for new appeared job
        #print('send notice for {}'.format(new_jids))
        for jid in new_jids:
            self.sendJobNotice (ts, jobs[jid])
            self.last_jobNotice[jid] = ts
        #send notice for dup job if long interval passed
        dup_jids_send = list(filter(lambda x: ts - self.last_jobNotice[x] > self.interval_secs, dup_jids))
        #print('{}: send notice for {} conditionly ({})'.format(ts, dup_jids_send, dup_jids))
        for jid in dup_jids_send:
            self.sendJobNotice(ts, jobs[jid])
            self.last_jobNotice[jid] = ts
        
    def sendJobNotice (self, ts, job):
        addr   ='http://scclin011:8126/jobDetails?jid={}'.format(job['job_id'])
        content=MSG_LOW_UTIL.format(job['job_id'], timedelta(seconds=ts - int(job['start_time'])), job['job_avg_util'], job['nodes'], addr)
        user   =MyTool.getUser(job['user_id'])
        #to_list=RECIPIENTS + ['@flatironinstitute.org'.format(user)]
        to_list=RECIPIENTS

        msg = EmailMessage()
        msg.set_content(content)
        msg['Subject'] = 'Long run job with low utilization at slurm cluster -- {}'.format(user)
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
