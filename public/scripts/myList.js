//q3 data binding need to arrays
function prepareDictData (data_dict, fields_dict) 
{
    if (!fields_dict)
       return Object.entries(data_dict)
    fields = Object.keys(fields_dict)
    f = fields.filter(function (k) {return data_dict.hasOwnProperty(k) && data_dict[k]})  //keep the order in fields
    return f.map(function (k) { return [fields_dict[k], data_dict[k], k] })
}

function createList  (data, fields, parent_id, type_dict, prepare_data_func=prepareDictData) 
{
        console.log('createList: orig data=', data, ',fields=', fields, ",type_dict=", type_dict)
        data = prepare_data_func(data, fields)
        var ul = d3.select(parent_id).append ('ul');
        ul.selectAll('li')
          .data(data).enter()
          .append('li')
          .html  (function(d) {
                     if (d[0]=='User') 
                        return '<span>' + d[0] + ':</span><a href=./userDetails?user=' + d[1]+'>' + d[1] + '</a>'
                     if (d[0]=='partitions'){ 
                       var str  = ''
                       for (pid of d[1])
                           str  = str + getPartDetailHtml(pid) + ' '
                       return '<span>' + d[0] + ':</span>' + str
                     }
                     if ((d[0]=='running_jobs') || (d[0]=='pending_jobs')) {
                       var str  = ''
                       for (jid of d[1])
                           str  = str + getJobDetailHtml(jid) + ' '
                       return '<span>' + d[0] + ':</span>' + str
                     }
                     if (type_dict && type_dict[d[2]]) {
                       if (type_dict[d[2]] == 'TresShort') {
                           return '<span>' + d[0] + ':</span>' + getTres_short(d[1])
                       }
                     }

                     return '<span>' + d[0] + ':</span>' + d[1]})
};
function getJobDetailHtml (jid) {
    return '<a href=./jobDetails?jid=' + jid + '>' + jid + '</a>'
}
function getPartDetailHtml (pid) {
    return '<a href=./partitionDetail?partition=' + pid +'>' + pid + '</a>'
}
function getTres_short(tres) {
    return tres.replace(/billing=\d+,/,'')
}
