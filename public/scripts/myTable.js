//typeof(data) is object and thus pass by reference
function prepareData (data) {
   return data
}
function getDisplayBps(n) {
   return getDisplayI(n) + ' Bps'
}
//input is nB
function getDisplayB (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n) 
   if (n < 1024)
      return n.toString() + ' B'
   n /= 1024
   return getDisplayK(n) + 'B'
}
function getDisplayI (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n) 
   if ( n < 1024)
      return n.toString() 
   return getDisplayK(n)
}
function getDisplayK (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n) 
   if (n < 1024) {
      if (Number.isInteger(n))
         return n.toString() + ' K'
      else
         return n.toFixed(2) + ' K'
   }
   n /= 1024
   if (n < 1024)
      return n.toFixed(2) + ' M'
   n /= 1024
   if (n < 1024)
      return n.toFixed(2) + ' G'
   n = n / 1024
   return n.toFixed(2) + ' T'
}
function getDisplayFloat (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n) 
   return n.toFixed(2)
}

//data_dict is a dictionary of a fixed format
function createMultiTable (data_dict, parent_id, table_title_list, job_id) {
   console.log(data_dict)
   console.log("table_title_list=", table_title_list)

   var pas = d3.select('#'+parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick')
      .html(function (d) {return d + ': alloc ' + data_dict[d][0] + ' CPUs, running processes ' + data_dict[d][1]+'<a href="./nodeJobProcGraph?node=' + d + '&jid=' + job_id + '"> (Proc Usage Graph) </a>'} )

   var tables = pas.append('table').property('id', function(d) {return d+'_proc'}).attr('class','noborder')
   var theads    = tables.append('thead')
                     .append('tr')
                     .selectAll('th')
                     .data(table_title_list)
                     .enter().append('th')
                        .attr('class','noborder')
                        .text(function (d,i) { return table_title_list[i]; })

   var trs    = tables.append('tbody').selectAll('tr')
                   .data(function (d) {return data_dict[d][5]})
                   .enter().append('tr')
                      .attr('class','noborder')
   trs.selectAll('td')
      .data(function(d) {return d})
      .enter().append('td')
         .attr('class', function(d,i) {return 'noborder ' + table_title_list[i]})
         .text(function (d) {return d}) //TODO: change to createMultiTable2 style
}

//data_dict is a dictionary of a fixed format
function createMultiTable2 (data_dict, parent_id, table_title_list, node, alloc_gpus, type_list) {
   console.log(data_dict)
   console.log("table_title_list=", table_title_list)

   var pas = d3.select('#'+parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick')
      .html(function (d) {
               var str= "Job " + d + ': '
               if ( d!= 'undefined') {
                  str += 'alloc ' + data_dict[d]["job"]["cpus_allocated"][node] + ' CPUs, ' + alloc_gpus + ' GPUs'
                  if (data_dict[d]["procs"] != undefined)
		     str += ', running processes ' + data_dict[d]["procs"].length +'<a href="./nodeJobProcGraph?node=' + node + '&jid=' + d + '"> (Proc Usage Graph) </a>'
                  else
		     str += ', no running processes.'
               }
               return str; })
   var tables = pas.append('table').property('id', function(d) {return d+'_proc'}).attr('class','noborder')
   var theads = tables.append('thead')
                     .append('tr')
                     .selectAll('th')
                     .data(table_title_list)
                     .enter().append('th')
                        .attr('class','noborder')
                        .text(function (d,i) { return table_title_list[i]; })

   var trs    = tables.append('tbody').selectAll('tr')
                   .data(function (d,i) {if (data_dict[d]["procs"]!=undefined) {return data_dict[d]["procs"]} else {return []}})
                   .enter().append('tr')
                      .attr('class','noborder')
   trs.selectAll('td')
      .data(function(d) {return d})
      .enter().append('td')
         .attr('class', function(d,i) {return 'noborder ' + table_title_list[i]})
         .html(function (d, i) {
                 if (type_list && type_list[i]) {
                    if (type_list[i] == 'B')
                       return getDisplayB(d) 
                    else if (type_list[i] == 'Bps')
                       return getDisplayBps(d)
                    else if (type_list[i] == 'Float')
                       return getDisplayFloat(d) 
                 }
             return d
         })
}

function createTable (data, titles_dict, table_id, parent_id, pre_data_func=prepareData, type_dict) {
        console.log("createTable data=", data, ",pre_data_fun=", pre_data_func, ",type_dict=", type_dict)
        var sortAscending = true;
        var table         = d3.select('#'+parent_id).append('table').property('id', table_id);
        var firstKey      = Object.keys(titles_dict)[0]   <!--use jobid as tie breaker for sorting -->

        pre_data_func (data)   //data is modified on site
        var headers = table.append('thead')
                           .append('tr')
                           .selectAll('th')
                           .data(Object.keys(titles_dict)).enter()
                           .append('th')
                           .attr('id', function(d) {return d})
                           .text(function (d) { return titles_dict[d]; })
                           .on('click', function (d) {
                               headers.attr('class', 'header');   //set all th's class
                               // must return 0, 1, -1, return true/false will not work
                               if (sortAscending) {
                                  rows.sort(function(a, b) {
                                      if (a[d] == b[d]) {
                                         if (b[firstKey]<a[firstKey]) {return 1;} else {return -1;}} 
                                      else {
                                         if (b[d] < a[d]) {return 1;} else {return -1;} } });
                                  sortAscending  = false;
                                  this.className = 'aes';
                               } else {
                                  rows.sort(function(a, b) { 
                                      if (a[d] == b[d]) {
                                         if (b[firstKey]<a[firstKey]) {return -1;} else {return 1;}} 
                                      else {
                                         if (b[d] < a[d]) {return -1;} else {return 1;} } });
                                  sortAscending  = true;
                                  this.className = 'des';
                               }
                            });
                  
        var rows = table.append('tbody').selectAll('tr')
                               .data(data).enter()
                               .append('tr')
                               .attr('data-group', function(d) {
                                  if ( d.data_group) return d.data_group ;})
                               .attr('data-group-idx', function(d) {
                                  if ( d.data_group) return d.data_group_idx;})
                               .attr('data-group-cnt', function(d) {
                                  if ( d.data_group) return d.data_group_cnt;})

        rows.selectAll('td')
            .data(function (d) {
                return Object.keys(titles_dict).map(function (k) {
                    if (d.data_group_cnt) 
                       return { 'value': d[k], 'name': k, 'group_cnt': d.data_group_cnt};
                    else
                       return { 'value': d[k], 'name': k};
                });
            }).enter()
            .append('td')
            .attr('data-th', function (d) {
                        return d.name; })
            .attr('group-cnt', function (d) {
                        if (d.group_cnt) return d.group_cnt; })
            .html(function (d) {
                 if (type_dict && type_dict[d.name]) {
                    if (type_dict[d.name] == 'Partition')
                       return '<a href=./partitionDetail?partition=' + d.value+'>' + d.value + '</a>'
                    else if (type_dict[d.name] == 'Time')
                       return getTS_string(d.value)
                    else if (type_dict[d.name] == 'TresShort')
                       return getTres_short(d.value)
                    else if (type_dict[d.name] == 'JobList') {
                       var jids = d.value.split(" ")
                       var str  = ''
                       for (jid of jids) 
                           str  = str + ' ' + getJobDetailHtml(jid)
                       return str
                    } else if (type_dict[d.name] == 'JobArray' && d.value) {
                       var str  = ''
                       for (jid of d.value) 
                           str  = str + ' ' + getJobDetailHtml(jid)
                       return str
                    } else if (type_dict[d.name] == 'JobName') {
                       return '<a href=./jobByName?name=' + d.value+'>' + d.value + '</a>'
                    } else if (type_dict[d.name] == 'JobAndStep') {
                       if ((d.value.toString().indexOf('.') == -1) && (d.value.toString().indexOf('_')==-1))   // not jobstep
                          return '<a href=./jobDetails?jid=' + d.value+'>' + d.value + '</a>'
                    } else if (type_dict[d.name] == 'Node') 
                       return getNodeDetailHtml (d.value)
                 }
                 if (d.name == 'user') { // TODO: change to use type_dict
                    return '<a href=./userDetails?user=' + d.value+'>' + d.value + '</a>'
                 } else if (d.name == 'partition') {  
                    return '<a href=./partitionDetail?partition=' + d.value+'>' + d.value + '</a>'
                 } else if ((d.name == 'job_id') || (d.name == 'id_job')) {
                    if ((d.value.toString().indexOf('.') == -1) && (d.value.toString().indexOf('_')==-1))   // not jobstep
                       return '<a href=./jobDetails?jid=' + d.value + '>' + d.value + '</a>'
                 }
                
                 return d.value;
            });
};
function getJobDetailHtml (jid) {
    return '<a href=./jobDetails?jid=' + jid + '>' + jid + '</a>'
}
function getNodeDetailHtml (id) {
    return '<a href=./nodeDetails?node=' + id + '>' + id + '</a>'
}
function getTS_string (ts) {
    var d = new Date(ts * 1000)
    //return d.toLocaleDateString() + ' ' + d.toLocaleTimeString()
    return d.toLocaleString()
}
function getTres_short(tres) {
    return tres.replace(/,billing=\d+/,'')
}

function prepareData_pending (data) {
   var savGID   = 'noGroup'
   var savGName = 'noGroup'
   var savGroup = []
   var group

   data.forEach (function (d) {
      group = d.user+d.partition+d.state_reason;
      //group = d.account; 

      if ( group != savGName) {
         // group changed, deal with saved ones
         savGroup.forEach(function (d) {
            d.data_group_cnt = savGroup.length
         });
         savGroup   = []
         savGName   = group
         savGID     = group + d.job_id
      }

      savGroup.push (d)
      d.data_group     = savGID
      d.data_group_idx = savGroup.length-1
   });
   savGroup.forEach(function (d) {
      d.data_group_cnt = savGroup.length
   });

   console.log ("prepareData_pending result=", data)
   return data
}

