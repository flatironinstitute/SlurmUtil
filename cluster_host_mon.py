#!/usr/bin/python

# Distributing via auto-update
# cp /mnt/home/apataki/prg/cperf/cluster_host_mon.py /cm/shared/sw/pkg/flatiron/cluster_host_mon/cluster_host_mon.py.NEW
# mv /cm/shared/sw/pkg/flatiron/cluster_host_mon/cluster_host_mon.py{.NEW,}

# Copying it back:
# cp /cm/shared/sw/pkg/flatiron/cluster_host_mon/cluster_host_mon.py /opt/cluster_mon/bin/cluster_host_mon.py
# systemctl restart cluster-host-mon


import importlib
import socket
import time
import re
import threading
import json
import sys
import os
import traceback
import signal
import argparse
import subprocess
import random
import xml.dom.minidom
import psutil

try:
    import paho.mqtt.client as mqtt
    mqtt_module = 'paho'
except ImportError:
    # Debian has the obsolete client ... need to handle that case
    import mosquitto as mqtt
    mqtt_module = 'old'


CLUSTER_HOST_MON_VERSION = '0.02.15'

#
# Application configuration options
#
app_config = {
    'mqtt_host': 'mon5.flatironinstitute.org',
    'script_path': '/opt/cluster_mon/bin/cluster_host_mon.py',
    'cluster_host_mon_update': '/cm/shared/sw/pkg/flatiron/cluster_host_mon/cluster_host_mon.py',
}


#
# Services to runon various nodes
#
service_config = [
    [ 'UpdateClusterHostMon',   '.*' ],
    [ 'HostInfo',               '.*' ],
    [ 'HostProcesses',          '.*' ],
    [ 'HostPerf',               '.*' ],
    [ 'VMInfo',                 '.*' ], 
    # [ 'PingTest', '.*' ],
    [ 'PkgInfo',                '.*' ],
    # [ 'DiskInfo', '.*' ],
    [ 'CephOsdTree',            'cephmon.*' ],
#    [ 'CephOsdVersion',         'cephmon.*' ],
    [ 'CephMdsClients',         'cephmds.*' ],
#    [ 'CephOsdDisk',            'cephosd.*' ],
#    [ 'CephOsdDisk',            'cephmds.*' ],
    [ 'DriveSMART',             '(ceph|xorph).*' ],
]


#
# Check to see if there is an update to the cluster_host_mon software
#
def check_update(log):

    update_path = app_config['cluster_host_mon_update']
    update_version = None

    log.log("check_update", "Checking for cluster_host_mon update at '{}'".format(update_path))

    try:
        f = open(update_path, 'r')
        for line in f:
            m = re.match(r"CLUSTER_HOST_MON_VERSION\s+=\s+'([.\d]+)'", line)
            if m:
                update_version = m.group(1)

    except IOError as e:
        log.log("check_update", "ERROR: Could not read file '{}'".format(update_path))
        return
            
    if update_version is None:
        log.log("check_update", "ERROR: Could not determine version of update script")
        return

    if update_version == CLUSTER_HOST_MON_VERSION:
        log.log("check_update", "No new update available")
        return

    log.log("check_update", "Updating from version '{}' to version '{}'".format(CLUSTER_HOST_MON_VERSION, update_version))
    retval = subprocess.call(["/usr/bin/cp", update_path, app_config['script_path']])
    
    if retval != 0:
        log.log("check_update", "Update FAILED (return code {})".format(retval))
        return

    log.log("check_update", "Restarting cluster_host_mon via systemctl")
    time.sleep(1)
    subprocess.call(["systemctl", "restart", "cluster-host-mon.service"])
    time.sleep(20)

    log.log("check_update", "Command 'systemctl restart cluster_host_mon.service' returned!!! (so restart failed)")    
    


class Log:

    def __init__(self, filename = None):
        if filename is None:
            self.fd = sys.stdout
        else:
            self.fd = open(filename, 'a', 0)

    def log(self, name, s):
        tstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.fd.write("{} {}: {}\n".format(tstr, name, s))


class ServiceBase(threading.Thread):

    def init_base(self, name, hostname, hostname_full, args, log, lock, mqtt_client):
        self.name = name
        self.hostname = hostname
        self.hostname_full = hostname_full
        self.args = args
        self.log_ = log
        self.lock = lock
        self.mqtt_client = mqtt_client
        self.running = False
        self.ok_timeout = 0
        self.mark_ok()


    def msg_add_hdr(self, H, msg_type):
        hdr = { 'msg_type': msg_type,
                'msg_ts': time.time(),
                'msg_process': 'cluster_host_mon',
                'hostname': self.hostname,
            }
        H['hdr'] = hdr
        self.log("Publishing {}".format(msg_type))


    def mark_ok(self):
        self.ok_ts = time.time()


    def log(self, s):
        self.log_.log(self.name, s)


    def dump_exception(self, e, comment = None):
        self.log('-' * 80)
        self.log("Exception caught in {}: {}".format(self.name, str(e)))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        L = traceback.format_exception(exc_type, exc_value, exc_traceback)
        for x in ''.join(L).split('\n'):
            self.log(x)
        #traceback.print_exc(file=self.log_.fd)
        if comment is not None:
            self.log(comment)
        self.log('-' * 80)


    def run_cmd(self, A, split = True, ignore_stderr = False, ignore_exit_code = False):

        self.cmd_line = A
        self.cmd_process = subprocess.Popen(A, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sout, serr = self.cmd_process.communicate()
        exit_code = self.cmd_process.returncode
        self.cmd_process = None
        self.cmd_line = A

        if not ignore_stderr:
            serr = serr.rstrip()
            if len(serr) != 0:
                raise Exception("{} returned: {}".format(' '.join(A), serr))

        if not ignore_exit_code:
            if exit_code != 0:
                raise Exception("{} exited with status: {}".format(' '.join(A), exit_code))

        R = { 'exit_code': exit_code,
              'serr': serr,
        }

        if split:
            L = sout.split('\n')
            R['sout'] = L
        else:
            R['sout'] = sout

        #@@@ to be changed to 'return R'
        return R['sout']


    def run(self):
        self.mark_ok()
        try:
            self.service()
        except Exception as e:
            self.dump_exception(e, "Thread is exiting ...")
        
        self.ok_ts = 0


    # Interrupt what the thread is doing from another thread forcibly
    def interrupt(self):
        # If we are running a subcommand, kill it
        if self.cmd_process is not None:
            self.log("Killing subprocess {}".format(' '.join(self.cmd_line)))
            self.cmd_process.kill()


    def run_periodically(self, fn, period, random_fraction = 0.1, allow_fails = 3):

        # Sanity check ...
        if random_fraction > 0.5:
            random_fraction = 0.5
        if random_fraction < 0.0:
            random_fraction = 0.0

        if self.ok_timeout <= 0:
            # Default: allow one failure
            self.ok_timeout = 2*period + 10

        # At the start, wait about half the period time
        period_randomized = period * random_fraction * (0.5 + 0.5*random.random())
        t0 = time.time()

        consecutive_fails = 0
        while self.running:
            t1 = time.time()
            if t1 - t0 >= period_randomized:
                try:
                    fn()
                except Exception as e:
                    consecutive_fails += 1
                    if (consecutive_fails > allow_fails):
                        self.dump_exception(e, "Failure #{} ({} consecutive failures allowed) - exiting thread".format(consecutive_fails, allow_fails))
                        raise
                    else:
                        self.dump_exception(e, "Failure #{} ({} consecutive failures allowed) - continuing".format(consecutive_fails, allow_fails))
                else:
                    consecutive_fails = 0
                    self.mark_ok()
                t0 = t1
                period_randomized = period * (1 + random_fraction * random.random())
            time.sleep(1)


#
#
#
class ServiceUpdateClusterHostMon(ServiceBase):

    def service_periodically(self):
        if self.args is not None and self.args.no_update:
            self.log("Not checking for updates to cluster_host_mon")
        else:
            check_update(self.log_)


    def service(self):
        self.run_periodically(self.service_periodically, 300)


#
#
#
class ServiceHostInfo(ServiceBase):

    def parse_cpuinfo(self):

        self.cpu_model = 'UNKNOWN'
        self.total_sockets = 0
        self.total_cores = 0
        self.total_threads = 0
        self.is_vm = 0

        phys_map = {}
        f = open('/proc/cpuinfo', 'r')
        for line in f:
            line = line.rstrip()
            A = re.split(r'\s*:\s*', line, 2)
            if len(A) == 2:
                k, v = A
                if k == 'processor':
                    processor = v
                if k == 'model name':
                    self.cpu_model = v
                if k == 'physical id':
                    physical_id = int(v)
                if k == 'siblings':
                    siblings = int(v)
                if k == 'cpu cores':
                    cpu_cores = int(v)
                if k == 'flags':
                    F = v.split()
                    if 'hypervisor' in F:
                        self.is_vm = 1
            
            if line == '':
                if physical_id not in phys_map:
                    self.total_cores += cpu_cores
                    self.total_threads += siblings
                    self.total_sockets += 1
                    phys_map[physical_id] = True

        f.close()

        #self.log("cpuinfo: cpu_model:{}, total_sockets:{}, total_cores:{}, total_threads:{}, is_vm:{}".format(
        #    self.cpu_model, self.total_sockets, self.total_cores, self.total_threads, self.is_vm))


    def parse_meminfo(self):

        self.mem = {}

        f = open('/proc/meminfo', 'r')
        for line in f:
            line = line.rstrip()
            L = re.split('[\s:]+', line)
            assert(len(L) >= 2 and len(L) <=3)
            if len(L) == 3:
                assert(L[2] == 'kB')
            self.mem[L[0]] = L[1]
            
        f.close()


    def parse_swappiness(self):
        
        f = open('/proc/sys/vm/swappiness', 'r')
        self.swappiness = int(f.readline().rstrip())
        f.close()


    def parse_network_settings(self):

        f = open('/proc/sys/net/ipv4/tcp_rmem', 'r')
        self.tcp_rmem = [ int(x) for x in f.readline().split() ]
        f.close()

        f = open('/proc/sys/net/ipv4/tcp_wmem', 'r')
        self.tcp_wmem = [ int(x) for x in f.readline().split() ]
        f.close()

        f = open('/proc/sys/net/ipv4/udp_mem', 'r')
        self.udp_mem = [ int(x) for x in f.readline().split() ]
        f.close()


    def parse_kernel_version(self):

        f = open('/proc/version', 'r')
        line = f.readline()
        L = line.split()
        assert(L[0] == 'Linux')
        assert(L[1] == 'version')
        self.kernel_version = L[2]
        f.close()

        f = open('/proc/uptime', 'r')
        L = f.readline().split()
        self.kernel_boot_ts = time.time() - float(L[0])
        f.close()


    def parse_os_version(self):
        
        self.os_version = 'UNKNOWN'

        # CentOS/Redhat/Fedora
        try:
            f = open('/etc/system-release', 'r')
            self.os_version = f.readline().rstrip()
            f.close()
            return
        except IOError:
            pass

        # Debian
        try:
            f = open('/etc/os-release', 'r')
            for line in f:
                line = line.rstrip()
                L = line.split('=')
                if L[0] == 'PRETTY_NAME':
                    self.os_version = L[1][1:-1]
            f.close()
            return
        except IOError:
            pass


    def parse_net_addr(self):

        self.net_if = []

        L = self.run_cmd(['/sbin/ip', 'addr', 'show'])
        net_ifname = None

        for line in L:
            line = line.rstrip()
            if len(line) == 0:
                continue

            m = re.match(r'\d+: ([\w@-]+):', line)
            if m:
                net_ifname = m.group(1)
                net_mac = None
                net_ip = None
                continue

            #if line[0] != ' ':
            #    print "line[0]:'{}'".format(line[0])
            assert(line[0] == ' ')

            m = re.match(r'\s+link/ether ([\w:]+)', line)
            if m:
                net_mac = m.group(1)

            m = re.match(r'\s+inet ([\d.]+)', line)
            if m:
                net_ip = m.group(1)
                if net_ifname is not None and net_mac is not None:
                    #print net_ifname, net_mac, net_ip
                    self.net_if.append({ "ifname": net_ifname,
                                         "mac": net_mac,
                                         "ip": net_ip,
                                     })


    def parse_dmidecode(self):

        self.hw_mem_max = 0
        self.hw_mem_installed = 0
        self.bios_vendor = ''
        self.bios_version = ''
        self.bios_date = ''
        self.system_vendor = ''
        self.system_model = ''
        self.system_serial = ''

        L = self.run_cmd("dmidecode")
        state = 0
        for line in L:

            # If the line starts with text at the beginning, then it is a top level entry
            if not line.startswith('\t') and not line.startswith(' '):
                state = 0

            line = line.strip()
            
            if state == 0:
                if line == 'Physical Memory Array':
                    state = 1
                if line == 'Memory Device':
                    state = 2
                if line == 'BIOS Information':
                    state = 3
                if line == 'System Information':
                    state = 4

            elif state == 1:

                m = re.match(r'Maximum Capacity:\s+(\d+)\s+(\w+)$', line)
                if m:
                    num = int(m.group(1))
                    unit = m.group(2)
                    if unit == 'TB':
                        self.hw_mem_max = num * 1024 * 1024 * 1024
                    elif unit == 'GB':
                        self.hw_mem_max = num * 1024 * 1024
                    elif unit == 'MB':
                        self.hw_mem_max = num * 1024
                    elif unit == "KB":
                        self.hw_mem_max = num
                    else:
                        self.log("ERROR: could not interpret unit '{}'".format(unit))

            elif state == 2:

                # Memory Device
                m = re.match(r'Size:\s+(\d+)\s+(\w+)$', line)
                if m:
                    num = int(m.group(1))
                    unit = m.group(2)
                    if unit == 'TB':
                        self.hw_mem_installed += num * 1024 * 1024 * 1024
                    elif unit == 'GB':
                        self.hw_mem_installed += num * 1024 * 1024
                    elif unit == 'MB':
                        self.hw_mem_installed += num * 1024
                    elif unit == "KB":
                        self.hw_mem_installed += num
                    else:
                        self.log("ERROR: could not interpret unit '{}'".format(unit))
                    state = 2
        
            elif state == 3:

                # BIOS Information
                m = re.match(r'Vendor:\s+(.*)', line)
                if m:
                    self.bios_vendor = m.group(1)

                m = re.match(r'Version:\s+(.*)', line)
                if m:
                    self.bios_version = m.group(1)

                m = re.match(r'Release Date:\s+(.*)', line)
                if m:
                    self.bios_date = m.group(1)

            elif state == 4:

                # System Information
                key, colon, value = line.strip().partition(':')
                value = value.strip()
                if key == 'Manufacturer':
                    self.system_vendor = value
                if key == 'Product Name':
                    self.system_model = value
                if  key == 'Serial Number':
                    if value != 'Not Specified':
                        self.system_serial = value

        if self.hw_mem_max == 0 or self.hw_mem_installed == 0:
            self.log("ERROR: could not interpret dmidecode output-2")
                        

    def publish_hostinfo(self):
        H = { 'hostname_full': self.hostname_full,
              'cpu': {
                  'cpu_model': self.cpu_model,
                  'total_sockets': self.total_sockets,
                  'total_cores': self.total_cores,
                  'total_threads': self.total_threads,
                  'is_vm': self.is_vm,
              },
              'os': {
                  'kernel_version': self.kernel_version,
                  'kernel_boot_ts': self.kernel_boot_ts,
                  'os_version': self.os_version,
              },
              'mem': {
                  'mem_total': int(self.mem['MemTotal']),
                  'swap_total': int(self.mem['SwapTotal']),
                  'swappiness': self.swappiness,
                  'hw_mem_installed': self.hw_mem_installed,
                  'hw_mem_max': self.hw_mem_max,
              },
              'system': {
                  'bios_vendor': self.bios_vendor,
                  'bios_version': self.bios_version,
                  'bios_date': self.bios_date,
                  'system_vendor': self.system_vendor,
                  'system_model': self.system_model,
                  'system_serial': self.system_serial,
              },
              'net': self.net_if,
              'tcp_rmem': self.tcp_rmem,
              'tcp_wmem': self.tcp_wmem,
              'udp_mem': self.udp_mem,
          }

        self.msg_add_hdr(H, 'cluster/hostinfo')
        with self.lock:
            self.mqtt_client.publish('cluster/hostinfo/' + self.hostname, json.dumps(H), retain=True)
            

    def service_periodically(self):
        self.parse_cpuinfo()
        self.parse_meminfo()
        self.parse_swappiness()
        self.parse_network_settings()
        self.parse_kernel_version()
        self.parse_os_version()
        self.parse_net_addr()
        self.parse_dmidecode()
        self.publish_hostinfo()


    def service(self):
        self.run_periodically(self.service_periodically, 600)



class ServiceHostPerf(ServiceBase):

    def read_loadavg(self):
        f = open('/proc/loadavg', 'r')
        line = f.readline();
        A = line.split()
        if len(A) != 5:
            raise Exception("/proc/loadavg has unrecognizable line: '{}'".format(line.strip()))
        self.load1 = float(A[0])
        self.load5 = float(A[1])
        self.load15 = float(A[2])
        
        B = A[3].split('/')
        if len(B) != 2:
            raise Exception("/proc/loadavg has unrecognizable line: '{}'".format(line.strip()))

        self.proc_total = int(B[1])

        # We remove ourselves from the run count
        self.proc_run = int(B[0]) - 1


    def read_hoststat(self):
        self.ct     = None
        self.vm     = None
        self.dio    = None
        self.nio    = None
        try:
            self.ct     = psutil.cpu_times()
        except Exception as e:
            self.log("ERROR: exception caught in psutil.cpu_times: " + str(e))

        try:
            self.vm     = psutil.virtual_memory()
        except Exception as e:
            self.log("ERROR: exception caught in psutil.virtual_memory: " + str(e))
            
        try:
            self.dio    = psutil.disk_io_counters()
        except Exception as e:
            self.log("ERROR: exception caught in psutil.disk_io_counters: " + str(e))

        try:
            self.nio    = psutil.net_io_counters()
        except Exception as e:
            self.log("ERROR: exception caught in psutil.net_io_counters: " + str(e))


    def get_hostperf_msg(self):

        H = { 
            'load': [ self.load1, self.load5, self.load15 ],
            'proc_total': self.proc_total,
            'proc_run': self.proc_run,
            'cpu_times': None,
            'mem': None,
            'disk_io': None,
            'net_io': None,
        }

        if self.ct is not None:
            H['cpu_times'] = {
                'user':          self.ct.user,
                'system':        self.ct.system,
                'idle':          self.ct.idle,
                'iowait':        self.ct.iowait,
            }

        if self.vm is not None:
            H['mem'] =  {
                'total':         self.vm.total,
                'used':          self.vm.used,
                'free':          self.vm.free,
                'buffers':       self.vm.buffers,
                'cached':        self.vm.cached,
                'available':     self.vm.available,
            }

        if self.dio is not None:
            H['disk_io'] = {
                'read_count':    self.dio.read_count,
                'read_bytes':    self.dio.read_bytes,
                'read_time':     self.dio.read_time,
                'write_count':   self.dio.write_count,
                'write_bytes':   self.dio.write_bytes,
                'write_time':    self.dio.write_time,
                #'busy_time':     self.dio.busy_time,
            }

        if self.nio is not None:
            H['net_io'] = {
                'tx_bytes':      self.nio.bytes_sent,
                'tx_packets':    self.nio.packets_sent,
                'tx_err':        self.nio.errout,
                'tx_drop':       self.nio.dropout,
                'rx_bytes':      self.nio.bytes_recv,
                'rx_packets':    self.nio.packets_recv,
                'rx_err':        self.nio.errin,
                'rx_drop':       self.nio.dropin,
            }

        self.msg_add_hdr(H, 'cluster/hostperf')
        return H


    def publish_procinfo(self):
        H = self.get_hostperf_msg()
        with self.lock:
            self.mqtt_client.publish('cluster/hostperf/' + self.hostname, json.dumps(H), retain=True)

    
    def service_periodically(self):
        self.read_loadavg()
        self.read_hoststat()
        self.publish_procinfo()


    def service(self):
        self.run_periodically(self.service_periodically, 30)



class ServiceHostProcesses(ServiceBase):
    def pid2slurm_jid(self, pid):
        fname = '/proc/{}/cgroup'.format(pid)
        if not os.path.exists(fname): return -1
        with open(fname, 'r') as f:
           for line in f.readlines():
               mObj = re.match('[0-9]+:cpuset:/slurm/uid_[0-9]+/job_([0-9]+)/', line)
               if mObj:
                  return int(mObj.group(1))
        return -1

    def read_processes(self):

        self.L = []
        for p in psutil.process_iter():
            try:
                uid = p.uids().effective
                if uid < 1000:
                    continue
                ct = p.cpu_times()
                mi = p.memory_info_ex()
                io = p.io_counters()
                slurm_jid = self.pid2slurm_jid(p.pid)
                self.L.append({ 'pid': p.pid,
                                'ppid': p.ppid(),
                                'uid': uid,
                                'name': p.name(),
                                'cmdline': p.cmdline(),
                                'status': p.status(),
                                'create_time': p.create_time(),
                                'num_threads': p.num_threads(),
                                'num_fds': p.num_fds(),
                                'cpu': {
                                    'user_time': ct.user,
                                    'system_time': ct.system,
                                    'affinity': p.cpu_affinity(),
                                },
                                'mem': {
                                    'vms': mi.vms,
                                    'rss': mi.rss,
                                    'shared': mi.shared,
                                    'text': mi.text,
                                    'lib': mi.lib,
                                    'data': mi.data,
                                },
                                'io': {
                                    'read_count': io.read_count,
                                    'write_count': io.write_count,
                                    'read_bytes': io.read_bytes,
                                    'write_bytes': io.write_bytes,
                                },
                                'jid': slurm_jid,

                })
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass


    def get_hostprocesses_msg(self):
        H = { 'processes': self.L }
        self.msg_add_hdr(H, 'cluster/hostprocesses')
        return H


    def publish_processes(self):
        H = self.get_hostprocesses_msg()
        with self.lock:
            self.mqtt_client.publish('cluster/hostprocesses/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.read_processes()
        self.publish_processes()


    def service(self):
        self.run_periodically(self.service_periodically, 90)


class ServicePkgInfo(ServiceBase):


    def check_yum_list(self, cmd, first_line):

        L = self.run_cmd(cmd, ignore_stderr = True, ignore_exit_code = True)
        assert(L[0] == first_line)

        L2 = []
        line_new = None
        for line in L:
            # Packages that we are planning to remove, we don't care about
            if line == 'Obsoleting Packages':
                break
            if len(line.rstrip()) == 0:
                continue
            if line[0] != ' ':
                if line_new is not None:
                    L2.append(line_new)
                line_new = line
            else:
                line_new += line

        if line_new is not None:
            L2.append(line_new)

        P = {}
        for line in L2[1:]:
            L = line.split()
            if len(L) != 3:
                #print "Unknown line: '{}'".format(line)
                continue
            P[L[0]] = L[1]

        return P


    def yum_cmd(self):
        if os.path.exists("/etc/fedora-release"):
            return "dnf"
        else:
            return "yum"


    def yum_list_installed(self):
        self.pkg_installed = self.check_yum_list([self.yum_cmd(), '-q', 'list', 'installed'], "Installed Packages")
        

    def yum_check_update(self):
        self.pkg_updates = self.check_yum_list([self.yum_cmd(), '-q', 'check-update'], '')


    def publish_pkginfo(self):
        H = { 'pkg_installed': self.pkg_installed,
              'pkg_updates': self.pkg_updates,
          }
        self.msg_add_hdr(H, 'cluster/pkginfo')
        with self.lock:
            self.mqtt_client.publish('cluster/pkginfo/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        if os.path.exists("/etc/redhat-release"):
            self.yum_list_installed()
            self.yum_check_update()
            self.publish_pkginfo()


    def service(self):
        self.ok_timeout = 5*3600+10
        self.run_periodically(self.service_periodically, 3600, allow_fails=10)


class ServiceVMInfo(ServiceBase):


    def decorate_vm_details(self, H):

        name = H['name']
        s = self.run_cmd(['virsh', 'dumpxml', name], split = False)

        dom = xml.dom.minidom.parseString(s)

        arch = 'UNKNOWN'
        machine = 'UNKNOWN'
        Ae1 = dom.getElementsByTagName('os')
        for e1 in Ae1:
            Ae2 = e1.getElementsByTagName('type')
            for e2 in Ae2:
                arch = str(e2.getAttribute('arch'))
                machine = str(e2.getAttribute('machine'))
        H['arch'] = arch
        H['machine'] = machine

        cpu_cores = -1
        cpu_pin_set = ''
        cpu_pin = 'none'
        Ae1 = dom.getElementsByTagName('vcpu')
        for e1 in Ae1:
            cpu_cores = int(e1.firstChild.nodeValue)
            cpu_pin_set = str(e1.getAttribute('cpuset'))
            if cpu_pin_set != '':
                if cpu_pin_set.startswith('0,2,4'):
                    cpu_pin = 'even'
                elif cpu_pin_set.startswith('1,3,5'):
                    cpu_pin = 'odd'
                else:
                    cpu_pin = 'other'
        H['cpu_cores'] = cpu_cores
        H['cpu_pin_set'] = cpu_pin_set
        H['cpu_pin'] = cpu_pin

        cpu_sockets = cpu_cores
        cpu_cores = 1
        cpu_threads = 1
        cpu_model = 'UNKNOWN'
        Ae1 = dom.getElementsByTagName('cpu')
        for e1 in Ae1:
            Ae2 = e1.getElementsByTagName('topology')
            for e2 in Ae2:
                cpu_sockets = int(e2.getAttribute('sockets'))
                cpu_cores = int(e2.getAttribute('cores'))
                cpu_threads = int(e2.getAttribute('threads'))
            Ae2 = e1.getElementsByTagName('model')
            for e2 in Ae2:
                cpu_model = str(e2.firstChild.nodeValue)
        H['cpu_sockets'] = cpu_sockets
        H['cpu_cores_per_socket'] = cpu_cores
        H['cpu_threads_per_core'] = cpu_threads
        H['cpu_model'] = cpu_model

        cpu_features = []
        Ae2 = e1.getElementsByTagName('feature')
        for e2 in Ae2:
            cpu_features.append(str(e2.getAttribute('name')))
        H['cpu_features'] = sorted(cpu_features)

        mem = -1
        mem_unit = 'KiB'
        Ae1 = dom.getElementsByTagName('memory')
        for e1 in Ae1:
            mem_unit = str(e1.getAttribute('unit'))
            mem = int(e1.firstChild.nodeValue)
        H['mem'] = mem
        H['mem_unit'] = mem_unit

        # The memory we are running with
        Ae1 = dom.getElementsByTagName('currentMemory')
        for e1 in Ae1:
            mem_unit = str(e1.getAttribute('unit'))
            mem = int(e1.firstChild.nodeValue)
        H['mem_run'] = mem
        H['mem_run_unit'] = mem_unit

        mac = 'UNKNOWN'
        Ae1 = dom.getElementsByTagName('mac')
        for e1 in Ae1:
            mac = str(e1.getAttribute('address'))
        H['mac'] = mac

        disk_img = 'UNKNOWN'
        Ae1 = dom.getElementsByTagName('disk')
        for e1 in Ae1:
            Ae2 = e1.getElementsByTagName('source')
            for e2 in Ae2:
                disk_img = str(e2.getAttribute('file'))
        H['disk_img'] = disk_img

        
    def run_virsh_list(self):

        self.VM = {}

        # If virtualization is not installed - do nothing
        if not os.path.exists("/usr/bin/virsh"):
            self.log("Not checking for virtualization (no virsh installed)")
            return

        try:
            L = self.run_cmd(['virsh', 'list', '--all'])
        except Exception as e:
            self.log("Not checking for virtualization ('virsh list --all' fails)")
            return

        try:
            F = L[0].split()
            assert(F[0] == 'Id')
            assert(F[1] == 'Name')
            assert(F[2] == 'State')
            assert(L[1][0] == '-')
            for line in L[2:]:
                line = line.rstrip()
                if len(line) == 0:
                    continue
                F = line.split()
                name = F[1]
                status = ' '.join(F[2:])
                H = {
                    'name': name,
                    'status': status,
                }
                self.decorate_vm_details(H)
                self.VM[F[1]] = H

        except Exception as e:
            self.dump_exception(e)


    def publish_vminfo(self):
        H = { 'vminfo': self.VM
          }
        self.msg_add_hdr(H, 'cluster/vminfo')
        with self.lock:
            self.mqtt_client.publish('cluster/vminfo/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.run_virsh_list()
        self.publish_vminfo()


    def service(self):
        self.run_periodically(self.service_periodically, 600)



class ServicePingTest(ServiceBase):

    # Ping servers in addition to ceph hosts
    ping_servers = [ 'hyperv000', 'hyperv001', 'hyperv002', 'hyperv003',
                     'hyperv004', 'hyperv005', 'hyperv006', 'hyperv007',
                     'hyperv008', 'hyperv009', 'hyperv010', 'hyperv011',
                     'hyperv012', 'hyperv013', 'hyperv014', 'hyperv015',
                     'hyperv016', 'hyperv029', 'hyperv030', 'hyperv031', ]

    def init(self):
        self.ceph_hosts_ts = 0
        self.ping_servers_ts = 0
        self.ping_results = []


    def eval_ping_test(self, H):

        status = 'ok'

        if H['pkt_loss'] >= 0.001:
            status = 'warn'
        if H['rtt_avg'] >= 0.5:
            status = 'warn'
        if H['rtt_max'] >= 10.0:
            status = 'warn'

        if H['pkt_loss'] >= 0.005:
            status = 'err'
        if H['rtt_avg'] >= 2.0:
            status = 'err'
        if H['rtt_max'] >= 50.0:
            status = 'err'
        
        H['status'] = status
        return status


    def run_ping_test(self, remote_hostname):
        # Send 10000 flood ping packets (or up to 5 seconds worth of packets) in a flood
        self.log("Running ping test to {}".format(remote_hostname))
        pkt_tx, pkt_rx, pkt_loss = 1, 0, 1.0
        rtt_min = rtt_avg = rtt_max = rtt_mdev = 0
        try:
            L = self.run_cmd(['/bin/ping', '-f', '-c', '100000', '-q', '-w', '5', remote_hostname])

            assert(L[0].startswith('PING'))
            assert(len(L[1].rstrip()) == 0)
            assert(L[2].startswith('---'))

            # Example lines:
            # 1000 packets transmitted, 1000 received, 0% packet loss, time 42ms
            # 6 packets transmitted, 0 received, +6 errors, 100% packet loss, time 2999ms
            m = re.match(r'(\d+) packets transmitted, (\d+) received.*(\d+)% packet loss, time (\d+)ms', L[3])
            if not m:
                self.log("Ping line '{}' failed to match regular expression".format(L[3]))
                assert(m)
            pkt_tx, pkt_rx = int(m.group(1)), int(m.group(2))
            pkt_loss = 1 - float(pkt_rx) / float(pkt_tx)

            # Example lines:
            # rtt min/avg/max/mdev = 0.029/0.032/0.243/0.009 ms, ipg/ewma 0.042/0.031 ms
            m = re.match(r'rtt min/avg/max/mdev = ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+) ms, ipg/ewma ([0-9.]+)/([0-9.]+) ms', L[4])
            if m:
                rtt_min, rtt_avg, rtt_max, rtt_mdev = float(m.group(1)), float(m.group(2)), float(m.group(3)), float(m.group(4))

        except Exception as e:
            # If the command didn't work - ignore
            self.dump_exception(e, "PING failed")

        H = { 'remote_hostname': remote_hostname,
              'ping_ts': time.time(),
              'pkt_tx': pkt_tx,
              'pkt_rx': pkt_rx,
              'pkt_loss': pkt_loss,
              'rtt_min': rtt_min,
              'rtt_avg': rtt_avg,
              'rtt_max': rtt_max,
              'rtt_mdev': rtt_mdev,
          }
        self.eval_ping_test(H)
        self.log("Ping results: pkt_tx:{}, pkt_rx:{}, pkt_loss:{:.2f}%, rtt_min:{}, rtt_avg:{}, rtt_max:{}, rtt_mdev:{}, status:{}".format(
            H['pkt_tx'], H['pkt_rx'], H['pkt_loss'], H['rtt_min'], H['rtt_avg'], H['rtt_max'], H['rtt_mdev'], H['status']))

        return H


    # Which ones are the ceph hosts?
    def get_ceph_hosts(self):
        self.ceph_hosts = []
        self.ceph_hosts_ts = time.time()
        self.we_are_ceph_host = False
        try:
            L = self.run_cmd(["/usr/bin/ceph", "osd", "tree"], ignore_stderr = True)
            for line in L:
                m = re.search('([\d.]+)\s+host\s+(\w+)', line)
                if m:
                    weight = float(m.group(1))
                    ceph_hostname = m.group(2)
                    if weight > 0:
                        self.ceph_hosts.append(ceph_hostname)
                        if ceph_hostname == self.hostname:
                            self.we_are_ceph_host = True

            self.ceph_hosts.sort()

            self.log("Ceph hosts: {}".format(' '.join(self.ceph_hosts)))
            if self.we_are_ceph_host:
                self.log("We are a ceph host")
            else:
                self.log("We are not a ceph host")


        except Exception as e:
            # If the command didn't work - ignore
            self.dump_exception(e)
            self.log("Could not get list of ceph hosts")
            pass

    # Which servers should we ping?
    def pick_ping_servers(self):
        self.ping_servers_ts = time.time()
        if self.we_are_ceph_host:
            self.ping_servers = [x for x in ServicePingTest.ping_servers]
            random.shuffle(self.ping_servers)
            self.ping_servers = self.ping_servers[:3]
            self.ping_servers.extend(self.ceph_hosts)
            random.shuffle(self.ping_servers)
        else:
            self.ping_servers = [x for x in self.ceph_hosts]
            self.ping_servers.extend(ServicePingTest.ping_servers)
            random.shuffle(self.ping_servers)
            self.ping_servers = self.ping_servers[:5]

        self.log("Picked servers to ping: {}".format(' '.join(self.ping_servers)))
        assert(len(self.ping_servers) > 0)


    def publish_results(self):
        H = { 'ping': self.ping_results,
          }
        self.msg_add_hdr(H, 'cluster/ping')
        with self.lock:
            self.mqtt_client.publish('cluster/ping/' + self.hostname, json.dumps(H), retain=True)

        
    def service_periodically(self):
        t = time.time()

        # Every so often we pick a new set of hosts to ping
        if t - self.ceph_hosts_ts > 3600:
            self.get_ceph_hosts()
            self.pick_ping_servers()
            self.ping_ix = 0

        if self.ping_ix >= len(self.ping_servers):
            self.ping_ix = 0

        H = self.run_ping_test(self.ping_servers[self.ping_ix])
        self.ping_ix += 1

        if H is not None:
            self.ping_results.append(H)
            if len(self.ping_results) > len(self.ping_servers):
                self.ping_results = self.ping_results[-len(self.ping_servers):]

            self.publish_results()


    def service(self):
        self.init()
        self.run_periodically(self.service_periodically, 450, 0.33)



class ServiceDiskInfo(ServiceBase):

    def get_linux_devices(self):

        self.linux_devices = {}

        # Find the linux devices in /dev
        L = os.listdir('/dev')
        for filename in sorted(L):
            if re.match(r'sd[a-z]+$', filename):
                device_name = '/dev/' + filename
                self.linux_devices[filename] = { 'linux_name': filename,
                                                    'linux_device': device_name, }

        # Find their WWN
        L = os.listdir('/dev/disk/by-id')
        for filename in L:
            m = re.match(r'wwn-0x(\w+)$', filename)
            if m:
                wwn1 = m.group(1)
                while len(wwn1) < 16:
                    wwn1 = '0' + wwn1
                wwn = wwn1[12:16] + wwn1[8:12] + wwn1[4:8] + wwn1[0:4]
                link = os.readlink('/dev/disk/by-id/' + filename)
                m = re.search('/(sd[a-z]+)$', link)
                if m:
                    linux_device = m.group(1)
                    self.linux_devices[linux_device]['wwn'] = wwn.lower()

        # Find their size
        for dev in self.linux_devices.itervalues():
            L = self.run_cmd(['/sbin/blockdev', '--getsize64', dev['linux_device']])
            dev['size'] = int(L[0])
            dev['sizeGB'] = dev['size'] // 1000000000

        # Find their locations
        L = os.listdir("/sys/bus/scsi/devices")
        for x in L:
            m = re.match(r'''\d+:''', x)
            if m:
                blockdir = "/sys/bus/scsi/devices/{}/block".format(x)
                L2 = []
                if os.path.exists(blockdir):
                    L2 = os.listdir(blockdir)
                    for line2 in L2:
                        if line2 not in self.linux_devices:
                            raise Exception("Drive {} is not in list of devices".format(line2))
                        self.linux_devices[line2]['adapter_addr'] = x


    def run_smartctl(self, dev):

        device = dev['linux_device']
        L = self.run_cmd(['/sbin/smartctl', '--all', device])

        H = {
            'raw_output': L,
            'device': device,
            'device_attr': {},
            'smart_attr': {},
            'controller_info': {},
        }

        state = 0
        for line in L:
            line = line.rstrip()

            if state == 0:
                if 'START OF INFORMATION SECTION' in line:
                    state = 1
                    continue

            if state == 1:
                if len(line) == 0:
                    state = 2
                    continue
                key, tmp, value = line.partition(':')
                key = key.strip()
                value = value.strip()
                
                if key == 'Device Model':
                    H['device_attr']['model'] = value
                elif key == 'Serial Number':
                    H['device_attr']['serial'] = value
                elif key == 'LU WWN Device Id':
                    H['device_attr']['wwn'] = value.translate(None, ' ').lower()
                elif key == 'Firmware Version':
                    H['device_attr']['firmware'] = value
                elif key == 'User Capacity':
                    m = re.match('([\d,]+) bytes', value)
                    if m:
                        bytes = int(m.group(1).translate(None, ','))
                        H['device_attr']['capacity'] = bytes
                elif key == 'Rotation Rate':
                    m = re.match('(\d+) rpm', value)
                    if m:
                        H['device_attr']['rotation'] = int(m.group(1))

            if state == 2:
                m = re.match('SMART overall-health self-assessment test result:\s*(.*)', line)
                if m:
                    H['smart_health'] = m.group(1)

                if line.startswith('ID#'):
                    state = 3
                    continue

            if state == 3:
                if len(line) == 0:
                    state = 4
                    continue

                A = line.split()
                attr_id = int(A[0])
                H['smart_attr'][attr_id] = {
                    'attr_id': attr_id,
                    'attr_name': A[1],
                    'flag': A[2],
                    'value': int(A[3]),
                    'worst': int(A[4]),
                    'threshold': A[5],
                    'type': A[6],
                    'updated': A[7],
                    'when_failed': A[8],
                    'raw_value': A[9]
                }

        dev['smart'] = H


    def query_megacli_adapters(self):

        L = self.run_cmd(['/scda/tools/sbin/MegaCli64', '-adpallinfo', '-aALL'])

        self.sas_adapters = []
        adapter = None

        for line in L:
            
            m = re.match(r'''Adapter #(\d+)''', line)
            if m:
                if adapter is not None:
                    self.sas_adapters.append(adapter)
                    #print(adapter)
                adapter = { 'id': int(m.group(1))
                        }

            m = re.match(r'''\s*([\w\s]+[^\s])\s*:\s+(.*[^\s])\s*''', line)
            if m:
                key = m.group(1)
                value = m.group(2)
                if key == 'Product Name':
                    adapter['name'] = value
                if key == 'Serial No':
                    adapter['serial_no'] = value
                if key == 'FW Package Build':
                    adapter['firmware'] = value
                if key == 'BIOS Version':
                    adapter['bios'] = value
                if key == 'Vendor Id':
                    adapter['vendor_id'] = value
                if key == 'Device Id':
                    adapter['device_id'] = value
                if key == 'SubVendorId':
                    adapter['sub_vendor_id'] = value
                if key == 'SubDeviceId':
                    adapter['sub_device_id'] = value
                if key == 'Virtual Drives':
                    adapter['virtual_drives_total'] = int(value)
                if key == 'Degraded':
                    adapter['virtual_drives_degraded'] = int(value)
                if key == 'Offline':
                    adapter['virtual_drives_offline'] = int(value)
                if key == 'Physical Devices':
                    adapter['physical_drives_total'] = int(value)
                if key == 'Failed Disks':
                    adapter['physical_drives_failed'] = int(value)
                    
        if adapter is not None:
            self.sas_adapters.append(adapter)
            #print(adapter)

        # Now find the PCI information
        L = self.run_cmd(['/scda/tools/sbin/MegaCli64', '-adpgetpciinfo', '-aALL'])

        adapter = None
        for line in L:
            m = re.search(r'''Controller (\d+)''', line)
            if m:
                adapter_id = int(m.group(1))
                if self.sas_adapters[adapter_id]['id'] != adapter_id:
                    raise Exception('Adapters not numbered sequentially!!!')
                adapter = self.sas_adapters[adapter_id]


            key, tmp, value = line.partition(':')
            key = key.strip()
            value = value.strip()

            if tmp == ':':
                if key == 'Bus Number':
                    bus_number = int(value, 16)
                if key == 'Device Number':
                    device_number = int(value, 16)
                if key == 'Function Number':
                    function_number = int(value, 16)
                    pci_addr = "{:02x}:{:02x}.{:01x}".format(bus_number, device_number, function_number)
                    adapter['pci_addr'] = pci_addr
    
                    # Look up the scsi host number
                    L2 = os.listdir('/sys/bus/pci/devices/0000:{}'.format(pci_addr))
                    for line2 in L2:
                        m = re.match(r'''host(\d+)''', line2)
                        if m:
                            adapter['scsi_host'] = int(m.group(1))
                            

    def query_megacli_logical_drives(self):

        L = self.run_cmd(['/scda/tools/sbin/MegaCli64', '-LDInfo', '-Lall', '-aALL'])

        self.sas_logical_drives = []

        adapter_id = None
        D = None

        for line in L:
            
            m = re.match(r'''Adapter (\d+)''', line)
            if m:
                adapter_id = int(m.group(1))

            m = re.match(r'''Virtual Drive: (\d+)''', line)
            if m:
                if D is not None:
                    self.sas_logical_drives.append(D)
                    #print (D)
                D = { 'adapter_id': adapter_id,
                      'logical_drive_id': int(m.group(1)),
                      'parity_size': '-',
                      'linux_dev': '',
                  }

            key, tmp, value = line.partition(':')
            key = key.strip()
            value = value.strip()

            if tmp == ':':
                if key == 'RAID Level':
                    D['raid_level'] = value
                if key == 'Size':
                    D['size'] = value
                if key == 'Parity Size':
                    D['parity_size'] = value
                if key == 'State':
                    D['state'] = value
                if key == 'Number Of Drives':
                    D['drive_count'] = int(value)

        if D is not None:
            self.sas_logical_drives.append(D)
            #print (D)
            

    def query_megacli_physical_drives(self):

        L = self.run_cmd(['/scda/tools/sbin/MegaCli64', '-PDList', '-aALL'])

        self.sas_physical_drives = []

        adapter_id = None
        D = None

        for line in L:

            m = re.match(r'Adapter #(\d+)', line)
            if m:
                adapter_id = int(m.group(1))

            key, tmp, value = line.partition(':')
            key = key.strip()
            value = value.strip()

            if tmp == ':':
                if key == 'Enclosure Device ID':
                    if D is not None:
                        self.sas_physical_drives.append(D)
                        #print(D)
                    D = { 'adapter_id': adapter_id, 'linux_dev': '' }

                if key == 'Device Id':
                    D['device_id'] = int(value)
                if key == 'Slot Number':
                    D['slot'] = int(value)
                if key == 'WWN':
                    D['wwn'] = value.lower()
                if key == 'Media Error Count':
                    D['media_error_count'] = int(value)
                if key == 'Inquiry Data':
                    D['device_id_str'] = value
                if key == 'Link Speed':
                    D['link_speed'] = value
                if key == 'Firmware state':
                    D['state'] = value
                if key == 'Drive Temperature':
                    m = re.match(r'''(\d+)C ''', value)
                    if not m:
                        raise Exception("Can not match drive temperature '{}'".format(value))
                    D['temperature'] = int(m.group(1))
                if key == "Drive's position":
                    m = re.match(r'''DiskGroup: (\d+), Span: 0, Arm: (\d+)''', value)
                    if not m:
                        raise Exception("Could not parse disk position '{}'".format(value))
                    D['raid-volume'] = int(m.group(1))
                    D['raid-position'] = int(m.group(2))
                    
        if D is not None:
            self.sas_physical_drives.append(D)
            #print(D)


    def megacli_match_to_linux(self):

        for adapter in self.sas_adapters:
        
            scsi_host = adapter['scsi_host']

            for drive in self.sas_physical_drives:
                if drive['adapter_id'] != adapter['id']:
                    continue
                    
                device_id = drive['device_id']

                linux_addr = '{}:{}:{}:{}'.format(scsi_host, 0, device_id, 0)
                
                for dev in self.linux_devices.itervalues():
                    #print dev['adapter_addr'], linux_addr
                    if dev['adapter_addr'] == linux_addr:
                        drive['linux_dev'] = dev['linux_name']
            
            for ldrive in self.sas_logical_drives:
                if ldrive['adapter_id'] != adapter['id']:
                    continue

                device_id = ldrive['logical_drive_id']

                linux_addr = '{}:{}:{}:{}'.format(scsi_host, 2, device_id, 0)
                
                for dev in self.linux_devices.itervalues():
                    #print dev['adapter_addr'], linux_addr
                    if dev['adapter_addr'] == linux_addr:
                        ldrive['linux_dev'] = dev['linux_name']
            

    def run_once(self):

        # Run smartctl on all devices
        self.get_linux_devices()

        for dev in self.linux_devices.itervalues():
            self.run_smartctl(dev)

        # Try to query the controller
        self.query_megacli_physical_drives()



    def publish_diskinfo(self):
        H = { 'linux_devices': self.linux_devices,
              'sas_physical_drives': self.sas_physical_drives,
              }
        self.msg_add_hdr(H, 'cluster/diskinfo')
        with self.lock:
            self.mqtt_client.publish('cluster/diskinfo/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):

        self.HW = self.run_once()
        self.publish_diskinfo()


    def service(self):
        self.run_periodically(self.service_periodically, 60)



#
# CephOsdTree
#
class ServiceCephOsdTree(ServiceBase):

    def run_osd_tree(self):

        self.osd_tree = []
        
        L = self.run_cmd(['ceph', 'osd', 'tree'])

        state = 0
        for line in L:
            if state == 0:
                if line.startswith('ID'):
                    state = 1
                    continue
            if state == 1:
                C = line.split()
                if len(C) <= 2:
                    continue
                if C[2] == 'host':
                    H = { 'hostname': C[3],
                          'weight': float(C[1]),
                          'osds': []
                      }
                    self.osd_tree.append(H)
                m = re.match(r'''osd.(\d+)''', C[2])
                if m:
                    osd_id = int(m.group(1))
                    H['osds'].append({ 'osd_id': osd_id,
                                       'weight': float(C[1]),
                                       'up_down': C[3],
                                       'reweight': float(C[4]),
                                       'primary_affinity': float(C[5]),
                                   })

        assert(state == 1)


    def publish_results(self):
        H = {
            'osd_tree': self.osd_tree,
        }
        self.msg_add_hdr(H, 'cluster/ceph/osd-tree')
        with self.lock:
            self.mqtt_client.publish('cluster/ceph/osd-tree/'  + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.run_osd_tree()
        self.publish_results()


    def service(self):
        self.run_periodically(self.service_periodically, 60)



#
# CephOsdVersion
#
class ServiceCephOsdVersion(ServiceBase):

    def run_osd_version(self):

        self.osd_version = []

        L = self.run_cmd(['ceph', 'tell', 'osd.*', 'version'])
        for line in L:
            m = re.match(r'''osd.(\d+)''', line)
            if m:
                osd_id = int(m.group(1))
            m = re.search(r'''ceph version ([0-9.]+)''', line)
            if m:
                osd_version = m.group(1)
                self.osd_version.append({'osd_id': osd_id,
                                         'osd_version': osd_version,
                                     })
                         

    def publish_results(self):
        H = {
            'osd_version': self.osd_version,
        }
        self.msg_add_hdr(H, 'cluster/ceph/osd-version')
        with self.lock:
            self.mqtt_client.publish('cluster/ceph/osd-version/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.run_osd_version()
        self.publish_results()


    def service(self):
        self.run_periodically(self.service_periodically, 60)



#
# CephMdsClients: find all the ceph clients and their versions
#
class ServiceCephMdsClients(ServiceBase):

    def find_mds(self):
        
        if not os.path.exists('/var/run/ceph'):
            return None

        L = os.listdir('/var/run/ceph')
        for filename in L:
            if filename.startswith('ceph-mds.'):
                return '/var/run/ceph/{}'.format(filename)

        return None


    def run_client_list(self):

        self.client_list = []

        mds_socket = self.find_mds()
        if mds_socket is None:
            return

        sout = self.run_cmd(['ceph', 'daemon', mds_socket, 'session', 'ls'], split = False)

        # Inactive MDS
        if 'mds_not_active' in sout.strip():
            return

        self.client_list = json.loads(sout)


    def publish_results(self):
        H = {
            'mds_clients': self.client_list,
        }
        self.msg_add_hdr(H, 'cluster/ceph/mds-clients')
        with self.lock:
            self.mqtt_client.publish('cluster/ceph/mds-clients/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.run_client_list()
        self.publish_results()


    def service(self):
        self.run_periodically(self.service_periodically, 15)



#
# CephOsdDisk
#
class ServiceCephOsdDisk(ServiceBase):

    def map_ceph_disks(self):

        self.ceph_disks = []

        L = os.listdir('/var/lib/ceph/osd')
        for filename in L:
            m = re.match(r'ceph-(\d+)$', filename)
            if m:
                osd_id = int(m.group(1))
                disk_path = os.readlink("/var/lib/ceph/osd/{}".format(filename))
                K = self.run_cmd(['df', disk_path])
                assert(K[0].startswith('Filesystem'))
                KL = K[1].split()
                fs_device = KL[0]
                fs_size = int(KL[1])
                fs_used = int(KL[2])
                self.ceph_disks.append({ 'osd_id': osd_id,
                                         'disk_path': disk_path,
                                         'fs_device': fs_device,
                                         'fs_size': fs_size,
                                         'fs_used': fs_used,
                                     })
                

    def publish_results(self):
        H = {
            'osd_disks': self.ceph_disks,
        }
        self.msg_add_hdr(H, 'cluster/ceph/osd-disk')
        with self.lock:
            self.mqtt_client.publish('cluster/ceph/osd-disk/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        self.map_ceph_disks()
        self.publish_results()


    def service(self):
        self.run_periodically(self.service_periodically, 60)

#
# DriveSMART
#
class ServiceDriveSMART(ServiceBase):

    SMARTCTL = "/opt/smartmontools/sbin/smartctl"

    def have_smartctl(self):
        return os.path.exists(ServiceDriveSMART.SMARTCTL)


    def scan_one_drive(self, device):

        # smartctl returns a non-zero exit status if the drive is failing ...
        s = self.run_cmd([ServiceDriveSMART.SMARTCTL, "-a" ,"-j", device], split=False, ignore_exit_code = True)
        D = json.loads(s)
        return D


    def scan_drives(self):
        
        s = self.run_cmd([ServiceDriveSMART.SMARTCTL, "--scan" ,"-j"], split=False)
        D = json.loads(s)
        return D


    def scan(self):

        H = { 'drives': {} }

        D = self.scan_drives()
        for d in D['devices']:
            name = d['name']
            if name.startswith('/dev/sd') or name.startswith('/dev/nvme'):
                D1 = self.scan_one_drive(name)
                H[name] = D1

        return H


    def publish_results(self, H):
        self.msg_add_hdr(H, 'cluster/drive-SMART')
        with self.lock:
            self.mqtt_client.publish('cluster/drive-SMART/' + self.hostname, json.dumps(H), retain=True)


    def service_periodically(self):
        if self.have_smartctl(): 
            H = self.scan();
            self.publish_results(H)


    def service(self):
        self.run_periodically(self.service_periodically, 600)



################################################################################c
# ClusterHostMon: application class
################################################################################

class ClusterHostMon:

    def init(self, args):

        self.version = CLUSTER_HOST_MON_VERSION
        log_filename = args.log_filename
        self.log_ = Log(log_filename)
        self.lock = threading.Lock()
        self.hostname_full = socket.gethostname()
        self.hostname = self.hostname_full.split('.')[0]
        self.action = None

        # Check to see if there is any update to this script
        if args is not None and args.no_update:
            self.log("Not checking for updates to cluster_host_mon")
        else:
            check_update(self.log_)

        signal.signal(signal.SIGTERM, self.sigterm_handler)

        # HOME needs to be defined correctly for some commands to run
        if 'HOME' not in os.environ:
            os.environ['HOME'] = '/tmp'
        
        self.log("Starting ClusterHostMon application version {}".format(self.version))


    def sigterm_handler(self,signo, stack_frame):
        self.action = 'shutdown-signal'


    def connect_mqtt(self):
        if mqtt_module == 'paho':
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.on_connect = self.on_connect
        else:
            self.mqtt_client = mqtt.Mosquitto()
            self.mqtt_client.on_connect = self.on_connect_old

        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(app_config['mqtt_host'])
        self.mqtt_client.loop_start()


    def disconnect_mqtt(self):
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()
    

    def on_connect(self, client, userdata, flags, rc):
        self.mqtt_client.subscribe("cluster/hostmon/control")

    def on_connect_old(self, client, userdata, rc):
        self.on_connect(client, userdata, None, rc)

    def on_message(self, client, userdata, msg):
        try:
            self.log("Received message {}".format(msg.topic))
            data = json.loads(msg.payload)
            if msg.topic == 'cluster/hostmon/control':
                self.process_control_msg(data)
        except Exception as e:
            self.log('-' * 80)
            self.log("Exception caught: " + str(e))
            L = traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback)
            for x in ''.join(L).split('\n'):
                self.log(x)
            #traceback.print_exc(file=self.log_.fd)
            self.log("Continuing ...")
            self.log('-' * 80)


    def msg_add_hdr(self, H, msg_type):
        hdr = { 'msg_type': msg_type,
                'msg_ts': time.time(),
                'msg_process': 'cluster_host_mon',
                'hostname': self.hostname,
            }
        H['hdr'] = hdr
        self.log("Publishing {}".format(msg_type))


    def process_control_msg(self, msg):

        # Make sure this host is a match for the control message
        matched = False
        if 'hostname' in msg:
            if self.hostname != msg['hostname']:
                return
            matched = True
        if 'hostmatch' in msg:
            if not re.match(msg['hostmatch'], self.hostname):
                return
            matched = True
        if 'hostall' in msg:
            matched = True
        if not matched:
            return

        action = msg['action']
        if action == 'shutdown':
            self.action = 'shutdown'
        if action == 'restart':
            self.action = 'restart'


    def log(self, s):
        self.log_.log("Main", s)


    def determine_services(self, args):

        SH = {}
        for service_name, host_regex in service_config:
            if re.match(host_regex, self.hostname):
                if service_name[0] == '!':
                    del SH[service_name[1:]]
                else:
                    SH[service_name] = True

        self.S = []
        for x in sorted(SH.keys()):
            self.log("Enabling thread: {}".format(x))
            service_class = getattr(importlib.import_module("__main__"), "Service" + x)
            service_obj = service_class()
            service_obj.init_base(x, self.hostname, self.hostname_full, args, self.log_, self.lock, self.mqtt_client)
            self.S.append(service_obj)


    def run_services(self):
        for s in self.S:
            if not s.running:
                s.running = True
                s.joined = False
                s.start()


    def dump_exception(self, e, comment = None):
        self.log('-' * 80)
        self.log("Exception caught: {}".format(str(e)))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        L = traceback.format_exception(exc_type, exc_value, exc_traceback)
        for x in ''.join(L).split('\n'):
            self.log(x)
        #traceback.print_exc(file=self.log_.fd)
        if comment is not None:
            self.log(comment)
        self.log('-' * 80)


    def install_updates(self):
        try:
            # Automatic updates are on redhat only (for now)
            if os.path.exists('/etc/redhat-release'):
                self.log("Attempting automatic update")
                proc = subprocess.Popen(['/scda/etc/cluster_host_mon/cluster_host_mon_update.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                sout, serr = proc.communicate()
                self.log(sout)
                self.log(serr)
        except Exception as e:
            self.dump_exception(e, "Self-update failed (ignoring) ...")



    def restart_program(self):
        self.log("Restarting application")
        python = sys.executable
        os.closerange(3, 1024)
        os.execl(python, python, *sys.argv)


    def shutdown_threads(self):
        for s in self.S:
            s.running = False

        # Wait for threads to shut down
        for ix in xrange(10):
            any_alive = False
            for s in self.S:
                if s.is_alive():
                    any_alive = True
                    if ix > 5:
                        # Try more actively after 5 seconds
                        s.interrupt()
                else:
                    if not s.joined:
                        s.join()
                        s.joined = True

            # If all threads have exited - we are done
            if not any_alive:
                return

            time.sleep(1)


    def publish_status(self, status):
        TH = {}
        t0 = time.time()
        all_ok = 1
        with self.lock:
            for s in self.S:
                ok = int(t0 - s.ok_ts < s.ok_timeout)
                THH = { 'name': s.name,
                        'alive': int(s.is_alive()),
                        'ok': ok,
                    }
                TH[s.name] = THH
                if ok == 0:
                    all_ok = 0
        
        H = { 'ok': all_ok,
              'status': status,
              'services': TH,
              'start_ts': self.start_ts,
              'app_version': self.version,
          }

        self.msg_add_hdr(H, "cluster/hostmon/status")

        with self.lock:
            self.mqtt_client.publish('cluster/hostmon/status/' + self.hostname, json.dumps(H), retain=True)


    def process_events(self):

        self.start_ts = time.time()
        self.publish_status("start")
        
        status_t0 = time.time()
        status_period = 300
        status_period_randomized = status_period * 0.1 * (0.5 + 0.5*random.random())
        while True:
            try:
                if self.action is not None:
                    self.log('Processing {} action'.format(self.action))
                    if self.action == 'shutdown' or self.action == 'shutdown-signal':
                        self.shutdown_threads()
                        break
                    if self.action == 'restart':
                        self.shutdown_threads()
                        break
                    self.log('Unknown action - ignoring')
                    self.action = None

                status_t1 = time.time()
                if status_t1 - status_t0 >= status_period_randomized:
                    self.publish_status("run")
                    status_t0 = status_t1
                    status_period_randomized = status_period * (1 + 0.1 * random.random())

                time.sleep(1)

            except KeyboardInterrupt as e:
                self.log("Keyboard interrupt")
                self.shutdown_threads()
                break

        self.publish_status("shutdown")
        self.log("Exiting ...")


    def run(self, args):

        self.init(args)
        self.connect_mqtt()
        self.determine_services(args)
        self.run_services()
        self.process_events()
        self.disconnect_mqtt()
        if self.action == 'restart':
            self.install_updates()
            self.restart_program()


def test_disk():
    x = ServiceDiskInfo()
    args = None
    x.init_base('Test', 'hyperv517', 'x', args, Log(), threading.Lock(), None)
    x.query_megacli_adapters()
    x.query_megacli_physical_drives()
    x.query_megacli_logical_drives()
    x.get_linux_devices()
    x.megacli_match_to_linux()

    for drive in x.sas_physical_drives:
        drive['marked'] = False

    for adapter in x.sas_adapters:
        print("Adapter {id}: {name}, firmware:{firmware}, bios:{bios}, serial:{serial_no}, pci:{pci_addr}, host:{scsi_host}".format(**adapter))
        
        for drive in x.sas_physical_drives:
            if drive['adapter_id'] != adapter['id']:
                continue

            if drive['state'] == 'JBOD':
                drive['marked'] = True

                print("    JBOD drive: slot:{slot:2d},  device-id:{device_id:2d},  temp:{temperature:2d}C,  errors:{media_error_count},  speed:{link_speed:8s},  wwn:{wwn},  model:{device_id_str},  dev:{linux_dev}".format(**drive))

        for ldrive in x.sas_logical_drives:
            if ldrive['adapter_id'] != adapter['id']:
                continue

            print("    Logical drive:{logical_drive_id},  state:{state},  #drives:{drive_count:2d},  size:{size},  parity-size:{parity_size},  RAID-level:{raid_level},  dev:{linux_dev}".format(**ldrive))

            for drive in x.sas_physical_drives:
                if drive['adapter_id'] != adapter['id']:
                    continue
                if 'raid-volume' not in drive or drive['raid-volume'] != ldrive['logical_drive_id']:
                    continue
                drive['marked'] = True
                print("        Physical drive: slot:{slot:2d},  device-id:{device_id:2d},  state:{state},  temp:{temperature:2d}C,  errors:{media_error_count},  speed:{link_speed:8s},  wwn:{wwn},  model:{device_id_str}".format(**drive))

        for drive in x.sas_physical_drives:
            if drive['adapter_id'] != adapter['id']:
                continue

            if not drive['marked']:
                drive['marked'] = True
                print("    Other drive: slot:{slot:2d},  device-id:{device_id:2d},  state:{state},  temp:{temperature:2d}C,  errors:{media_error_count},  speed:{link_speed:8s},  wwn:{wwn},  model:{device_id_str}".format(**drive))


    def sortkey(d):
        x = d['linux_name']
        if len(x) == 3:
            return '0' + x[2]
        else:
            return x[2:4]

    print()
    print("Linux devices:")
    for linux_dev in sorted(x.linux_devices.itervalues(), key=sortkey):
          print("    {linux_name:4s}:  size:{sizeGB:5d} GB,  wwn:{wwn},  addr:{adapter_addr}".format(**linux_dev))
        


def test_ceph():
    x = ServiceCephOsd()
    x.init_base('Test', 'hyperv030', 'x', Log(), threading.Lock(), None)
    x.run_client_list();
    print x.client_list

def test1():
    x = ServiceDriveSMART()
    x.init_base('Test', 'cephosd000', 'cephosd000', 'x', Log(), threading.Lock(), None)
    x.scan()
    #print json.dumps(x.L, sort_keys=True, indent=4, separators=(',', ': '))

def test_HostProcesses():
    x = ServiceHostProcesses ()
    x.read_processes ()
    r = [(p['pid'], p['uid'], p['jid']) for p in x.L]
    print('test_HostProcesses {}'.format(r))

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-filename', default=None, help='Name of the log file to append to')
    parser.add_argument('--no-update', default=False, action='store_const', const = True,
                        help = 'Do not attempt to update the process automatically')

    args = parser.parse_args()

    app = ClusterHostMon()
    app.run(args)


if __name__ == "__main__":
   main()
#test1()
