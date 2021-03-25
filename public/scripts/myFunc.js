function getTresWithoutBilling(tres) {
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
    tres_str = tres_str.replace(/2=(\d+)000/, 'mem=$1G')
    return     tres_str.replace('1=',         'cpu=')
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



