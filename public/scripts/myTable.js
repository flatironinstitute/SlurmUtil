//typeof(data) is object and thus pass by reference
function prepareData (data) {
   return data
}

//data_dict is a dictionary of a fixed format
function createMultiTable (data_dict, parent_id, table_title_list, job_id) {
   console.log(data_dict)
   console.log(parent_id)
   console.log(table_title_list)

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
         .text(function (d) {return d})
}

//data_dict is a dictionary of a fixed format
function createMultiTable2 (data_dict, parent_id, table_title_list, node, alloc_gpus) {
   console.log(data_dict)
   console.log(parent_id)
   console.log("table_title_list")
   console.log(table_title_list)

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
      .data(function(d) {console.log(d); return d})
      .enter().append('td')
         .attr('class','noborder')
         .text(function (d) {return d})
}

function createMultiList (data_dict, parent_id) {
   console.log(data_dict)
   console.log(parent_id)
   console.log(d3.select('#'+parent_id))
   var pas = d3.select('#'+parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('p')
               .text(function (d) {console.log(d); return d + ': alloc cores ' + data_dict[d][0] + ' , running processes ' + data_dict[d][1]} )

   var uls = pas.append('ul').property('id', function(d) {return d+'_proc'})
   uls.selectAll('li')
       .data(function (d) {return data_dict[d][5]})
       .enter().append('li')
          .text(function(d) { return d })
}

function createNestedTable (data, field_key, title_dict, sub_data, table_id, parent_id) {
        console.log('createNestedTable')
        console.log(data)
        console.log(sub_data)
        var col_cnt = Object.keys(title_dict).length 
        var table  = d3.select('#'+parent_id).append('table').property('id', table_id);
        var header = table.append('thead')
                          .append('tr')
                          .selectAll('th')
                          .data(Object.keys(title_dict))
                          .enter().append('th')
                          .text(function (d,i) { return title_dict[i]; })
        var enterRows   = table.append('tbody').selectAll('tr')
                          .data(data).enter()
        enterRows.append('tr')
            .selectAll('td')
            .data(function (d) {
                return Object.keys(title_dict).map(function (k) {
                    return { 'value': d[k], 'name': k};
                });
             })
            .enter().append('td')
            .attr('data-th', function (d) {
                        return d.name; })
            .html(function (d) {
                 if (d.name == 'user') {
                    return '<a href=./userJobs?user=' + d.value+'>' + d.value + '</a>'
                 } else if (d.name == 'partition') {
                    return '<a href=./partitionDetail?partition=' + d.value+'>' + d.value + '</a>'
                 } else if (d.name == 'job_id') {
                    return '<a href=./jobDetails?jid=' + d.value + '>' + d.value + '</a>'
                 }
                 return d.value;})

         var subRows = enterRows.insert('tr')
            .attr('class', function(d) {return d['job_id'] + ' worker_proc';})
            .append('td')
            .attr('colspan', col_cnt)
         subRows.append('table')
                .attr('class', 'nested_table')
                .selectAll('tr')
                .data(function (d) {return d['nodes_flat']})
                .enter().append('tr').append('td')
                   .text(function (d) {return d;})
                
};
             
function createTable (data, titles_dict, table_id, parent_id, pre_data_func=prepareData) {
        console.log("createTable data=", data, ",pre_data_fun=", pre_data_func)
        var sortAscending = true;
        var table         = d3.select('#'+parent_id).append('table').property('id', table_id);
        var firstKey      = Object.keys(titles_dict)[0]   <!--use jobid as tie breaker for sorting -->

        pre_data_func (data)
        var headers = table.append('thead')
                           .append('tr')
                           .selectAll('th')
                           .data(Object.keys(titles_dict)).enter()
                           .append('th')
                           .text(function (d) { return titles_dict[d]; })
                           .on('click', function (d) {
                               console.log ('on click ' + d)
                               headers.attr('class', 'header');
                                           
                               // must return 0, 1, -1, return true/false will not work
                               if (sortAscending) {
                                  rows.sort(function(a, b) {
                                      console.log('cmp ' + d + ' value ' + JSON.stringify(b[d]) + '<' + JSON.stringify(a[d])); 
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
                    return { 'value': d[k], 'name': k};
                });
            }).enter()
            .append('td')
            .attr('data-th', function (d) {
                        return d.name; })
            .html(function (d) {
                 if (d.name == 'user') {
                    return '<a href=./userJobs?user=' + d.value+'>' + d.value + '</a>'
                 } else if (d.name == 'partition') {
                    return '<a href=./partitionDetail?partition=' + d.value+'>' + d.value + '</a>'
                 } else if ((d.name == 'job_id') || (d.name == 'id_job')) {
                    return '<a href=./jobDetails?jid=' + d.value + '>' + d.value + '</a>'
                 }
                
                 return d.value;
            });
};

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

