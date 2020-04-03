function getTresWithoutBilling(tres) {
    return tres.replace(/billing=\d+,/,'')
};
function getJobDetailHtml (jid) {
    return '<a href=./jobDetails?jid=' + jid + '>' + jid + '</a>'
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
function getNodeDetailHtml (id) {
    return '<a href=./nodeDetails?node=' + id + '>' + id + '</a>'
}
function getQoSDetailHtml (id) {
    return '<a href=./qosDetail?qos=' + id + '>' + id + '</a>'
}
function getTS_string (ts) {
    var d = new Date(ts * 1000)
    return d.toLocaleString('en-US', {hour12:false})
}
function getTresReplaceInteger(tres_str) {
    tres_str = tres_str.replace('1001=',      'gres/gpu=')
    tres_str = tres_str.replace('4=',         'node=')
    tres_str = tres_str.replace(/2=(\d+)000/, 'mem=$1G')
    return     tres_str.replace('1=',         'cpu=')
}

