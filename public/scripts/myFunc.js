function getTresWithoutBilling(tres) {
    return tres.replace(/billing=\d+,/,'')
};
function getTresDisplay(tres) {
    //TRES_KEY_MAP={1:'cpu',2:'mem',4:'node',1001:'gpu'}
    if (tres.search("1=") >= 0) {
       tres = tres.replace("1=", "cpu=") 
       tres = tres.replace("2=", "mem=") 
       tres = tres.replace("4=", "node=") 
       tres = tres.replace("5=", "billing=") 
       tres = tres.replace("1001=", "gpu=") 
    }
    return tres.replace(/billing=\d+,/,'')
};
function getJobDetailHtml (jid) {
    return '<a href=./jobDetails?jid=' + jid + '>' + jid + '</a>'
};
function getJobListHtml(lst) {
    var html_list = lst.map( function(p) {return getJobDetailHtml(p);} )
    return html_list.join(', ')
}
function getJobListSummaryHtml(lst) {
    var detail_lst = (lst.length>10) ? lst.slice(0,10) : lst
    var html_list = detail_lst.map( function(p) {return getJobDetailHtml(p);} )
    var html_str  = html_list.join(',')
    return (lst.length>10) ? html_str + '... total ' + lst.length + ' jobs' : html_str
}
function getJobNameHtml(name) {
    return '<a href=./jobByName?name=' + name + '>' + name + '</a>'
}
function getJob_StepHtml(d_value) {
    if ((d_value.toString().indexOf('.') == -1) && (d_value.toString().indexOf('_')==-1))   // not jobstep
       return '<a href=./jobDetails?jid=' + d_value+'>' + d_value + '</a>'
    return d_value
}

function getUserDetailHtml (user) {
    return '<a href=./userDetails?user=' + user + '>' + user + '</a>'
};
function getJobArrayDetail (jids_array) {
    var str  = ''
    for (jid of jids_array)
        str  = str + ' ' + getJobDetailHtml(jid)
    return str
}
function getPartDetailHtml (pid) {
    return '<a href=./partitionDetail?partition=' + pid +'>' + pid + '</a>'
}
function getPartAvailString(p) {
    var str = p.name + " (cpu=" + p.user_avail_cpus + ",node=" + p.user_avail_nodes 
    if (p.user_avail_gpus == 0)
       return str + ")"
    else
       return str + ",gpu=" + p.user_avail_gpus + ")"
}
function getPartListAvailString (p_list) {
    console.log("getPartListAvailString", p_list)
    var str_list = p_list.map( function(p) {return getPartAvailString(p);} )
    return str_list.join(', ')
}
function getPartitionListHtml(p_list) {
    var html_list = p_list.map( function(p) {return getPartDetailHtml(p);} )
    return html_list.join(',')
}
function getNodeDetailHtml (id) {
    return '<a href=./nodeDetails?node=' + id + '>' + id + '</a>'
}
function getQoSDetailHtml (id) {
    return '<a href=./qosDetail?qos=' + id + '>' + id + '</a>'
}
//return 2/23/2021, 10:18:02
function getTS_LString (ts_sec) {
    var d = new Date(ts_sec * 1000)
    return d.toLocaleString('en-US', {hour12:false})
}
function getTresReplaceInteger(tres_str) {
    tres_str = tres_str.replace('1001=',      'gres/gpu=')
    tres_str = tres_str.replace('4=',         'node=')
    tres_str = tres_str.replace(/2=(\d+)/,    'mem=$1M')
    return     tres_str.replace('1=',         'cpu=')
}
function getPeriodDisplay(secs){
    var day = Math.floor(secs / (24*3600))
    var rem = secs % (24*3600)
    var d   = new Date(rem * 1000)
    var hms = d.toISOString().substr(11, 8)  //00:00:00
    if (day > 0) {
       year = Math.floor(day / 365)
       day  = day % 365
       d_s  = day + '-' + hms
       if (year > 0)
          return year + "Y-" + d_s 
       else
          return d_s;
    } else
       return hms;
}

function getTresUsage_1(dict) {
    var alloc_sec  = dict['alloc_secs']
    var rank       = dict['rank']
    if (rank<=10)
       return getPeriodDisplay(alloc_sec) + " (#" + rank + " top user)";
    else
       return getPeriodDisplay(alloc_sec);
}

function getTresUsageString(tres_dict) {
    if ((!tres_dict) || Object.keys(tres_dict).length==0)
       return ''
    var cpu_str  = "cpu="  + getTresUsage_1(tres_dict[1])
    var node_str = "node=" + getTresUsage_1(tres_dict[4])
    var mem_str =  "mem(MB)="  + getTresUsage_1(tres_dict[2])
    if (1001 in tres_dict) {
       var gpu_str =  "gpu="  + getTresUsage_1(tres_dict[1001])
       return cpu_str+", " + node_str+", " + mem_str+", " + gpu_str
    } else
       return cpu_str+", " + node_str+", " + mem_str
}

function getFileUsageString(usage_dict) {
    console.log("usage_dict=", usage_dict)
    var home_str = 'home=' + usage_dict['home']
    if ('ceph' in usage_dict) {
       var ceph_str = ', ceph=' + getDisplayN (usage_dict['ceph'][1])
       var rank     = usage_dict['ceph'][2]
       if (rank>10)
          return home_str + ceph_str
       else
          return home_str + ceph_str + " (#" + rank + " top user)" 
    } else
       return home_str
}

function getDisplayN (n) {
   //console.log('getDisplayN', n)
   if (typeof (n) != 'Number')
      n = parseFloat (n)
   if (n < 1024) {
      if (Number.isInteger(n))
         return n.toString()
      else
         return n.toFixed(2)
   }
   return getDisplayK (n/1024)
}
//display 12390(K) as 12.39 M
function getDisplayK (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n)
   if (n < 1024) return getDisplayF(n) + 'K'
   n /= 1024
   return getDisplayM(n)
}
function getDisplay_MB (n) {
   return getDisplayM (n) + 'B'
}
function getDisplayM (n) {
   if (typeof (n) != 'Number')
      n = parseFloat(n)
   if (n < 1024) return getDisplayF(n) + 'M'
   n /= 1024
   if (n < 1024) return getDisplayF(n) + 'G'
   n = n / 1024
   return getDisplayF(n) + 'T'
}
function getDisplayF (n) {
   if (Number.isInteger(n))
      return n.toString()
   else
      return n.toFixed(2)
}
function getPadStr (s) {
   return ("0" + s).slice(-2)
}
function getDateString (d) {
   return d.getFullYear() + "-" + getPadStr(d.getMonth()+1) + '-' + getPadStr(d.getDate()) 
}
function getTimeString(d) {
   return getPadStr(d.getHours()) + ":" + getPadStr(d.getMinutes())
}
function getDateTimeString(d) {
   return getDateString (d) + "T" + getTimeString(d)
}



