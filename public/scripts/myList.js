//q3 data binding need to arrays
//return [[display_text, value,key],...]
function prepareDictData (data_dict, fields_dict) 
{
    if (!fields_dict)
       return Object.entries(data_dict)
    fields = Object.keys(fields_dict)
    f = fields.filter(function (k) {return data_dict.hasOwnProperty(k) && data_dict[k]})  //keep the order in fields
    return f.map(function (k) { return [fields_dict[k], data_dict[k], k] })
}

//data={key:value}, fields={key:display_text}, type_dict={key:display_type} are all objects(python dict)
function createList  (data, fields, parent_id, type_dict, prepare_data_func=prepareDictData) 
{
        console.log('createList: orig data=', data)
        data = prepare_data_func(data, fields)
        var ul = d3.select(parent_id).append ('ul');
        ul.selectAll('li')
          .data(data).enter()
          .append('li')
          .html  (function(d) {
                     var span_str = '<span class="attribute">'
                     if (d[0]=='partitions'){ 
                       var str  = ''
                       for (pid of d[1])
                           str  = str + getPartDetailHtml(pid) + ' '
                       return span_str + d[0] + ':</span>' + str
                     }
                     if ((d[0]=='running_jobs') || (d[0]=='pending_jobs')) {
                       var str  = ''
                       for (jid of d[1])
                           str  = str + getJobDetailHtml(jid) + ' '
                       return span_str + d[0] + ':</span>' + str
                     }
                     if (type_dict && type_dict[d[2]]) {
                       if (type_dict[d[2]] == 'TresShort') {
                           return span_str + d[0] + ':</span>' + getTresWithoutBilling(d[1])
                       }else if (type_dict[d[2]] == 'Time') {
                           return span_str + d[0] + ':</span>' + getTS_LString(d[1])
                       }else if (type_dict[d[2]] == 'Mem_MB') {
                           return span_str + d[0] + ':</span>' + d[1] + 'MB'
                       }else if (type_dict[d[2]] == 'JobArray' && d[1]) {
                           return span_str + d[0] + ':</span>' + getJobArrayDetail(d[1])
                       }else if (type_dict[d[2]] == 'User' && d[1]) {
                           return span_str + d[0] + ':</span>' + getUserDetail(d[1])
                       }else if (type_dict[d[2]] == 'QoS' && d[1]) {
                           return span_str + d[0] + ':</span>' + getQoSDetailHtml(d[1])
                       }else if (type_dict[d[2]] == 'TresInteger' && d[1]) 
                           return span_str + d[0] + ':</span>' + getTresReplaceInteger(d[1])
                     } 

                     return span_str + d[0] + ':</span>' + d[1]})
};
