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
   n = n / 1024
   return getDisplayK(n)
}
function getDisplayFloat (n) {
   if (typeof (n) != 'Number')
      n = parseFloat (n)
   if (n < 1024)
      return n.toFixed(2)
   else {
      n /= 1024
      return getDisplayK(n)
   }
}

function createMultiTitle (data_dict, parent_id, job_id, cluster=DEF_CLUSTER) {
   var pas = d3.select(parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick table_title')
      .html(function (d) {
               var procLink = '<a href="' + getNodeJobProcHref(d, job_id, cluster) + '"> (Proc Usage Graph) </a>'
               var rlt      = getNodeDetailHtml(d, cluster) + ': ' + data_dict[d][1] + ' processes'
               if (data_dict[d][0]>0)
                  rlt  = rlt + ' on ' + data_dict[d][0] + ' CPUs, Avg Inst. CPU util ' + data_dict[d][2].toFixed(2)
               rlt          = rlt + procLink
               return rlt})
   return pas
}

//data_dict is a dictionary of a fixed format
function createMultiTable (data_dict, parent_id, table_title_list, job_id, no_head=false, cluster=DEF_CLUSTER) {
   console.log("createMultiTable: data_dict=", data_dict, ", table_title_list=", table_title_list)
   var pas    = createMultiTitle (data_dict, parent_id, job_id, cluster)
   var tables = pas.append('table').property('id', function(d) {return d+'_proc'}).attr('class','noborder')
   if (!no_head)
      var theads = tables.append('thead')
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

   collapseTitle ()
} // createMultiTable

function createMultiTitle2 (data_dict, parent_id, node) {
   var pas = d3.select(parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick table_title')
      .html(function (d) {
               var str= "Job " + d + ': '
               if ( d!= -1) {
                  var job = data_dict[d]["job"]
                  str += 'alloc ' + job["cpus_allocated"][node] + ' CPUs'
                  if (("gpus_allocated" in job) && (node in job["gpus_allocated"]))
                     str += ', ' + job["gpus_allocated"][node].length + ' GPUs'
                  if (data_dict[d]["procs"] != undefined)
		     str += ', have ' + data_dict[d]["procs"].length +' processes. <a href="./nodeJobProcGraph?node=' + node + '&jid=' + d + '"> (Proc Usage Graph) </a>'
                  else
		     str += ', have no running processes.'
               } else {
                   str = "Processes:"
               }
               return str; })
    return pas
}

function collapseTitle () {
   // collapse behavior
   $('.table_title').toggleClass('table_title_collapse')
   $('.table_title').next().toggleClass('hide')
   $('.table_title').click(function(event) {
       //$(this).children("i").toggleClass('hide')
      $(this).toggleClass('table_title_collapse')
      $(this).next().toggleClass('hide')
   });
}

function expandTitle () {
   $('.table_title').click(function(event) {
       $(this).toggleClass('table_title_collapse')
       $(this).next().toggleClass('hide')
       });
}

//create process table in a node
function createProcTable (data_dict, parent_id, table_title_list, node, type_list, idx_list, cluster=DEF_CLUSTER) {
   console.log("createProcTable: data_dict=", data_dict, "table_title_list=", table_title_list, "node=", node, "type_list=", type_list)
   var table = d3.select(parent_id).append('table').property('id', node+'_proc').attr('class','noborder');
   var trs   = table.append('tbody').selectAll('tr')
                     .data(Object.values(data_dict)).enter()
                     .append('tr')
                     .attr('class','noborder')
   trs.selectAll('td')
      .data(function(d) {return idx_list.map(x => d.at(x))})
      .enter().append('td')
         .attr('class', function(d,i) {return 'noborder ' + table_title_list[i]})
         .html(function (d, i) {
              return getTypedValueHtml(d, type_list[i], cluster);
         })

   // collapse behavior
   var thead = createSortTableHeader_1(table, table_title_list, idx_list, trs)
   collapseTitle ()
}

function createJobProcTable (data_dict, parent_id, table_title_list, node, type_list, idx_list, cluster=DEF_CLUSTER) {
   console.log("createJobProcTable: data_dict=", data_dict, "table_title_list=", table_title_list, "node=", node, "type_list=", type_list)
   var pas    = createMultiTitle2(data_dict, parent_id, node)
   var tables = pas.append('table').property('id', function(d) {return d+'_proc'}).attr('class','noborder')
   var theads = tables.append('thead')
                     .append('tr')
                     .selectAll('th')
                     .data(table_title_list)
                     .enter().append('th')
                        .attr('class','noborder')
                        .text(function (d,i) { return table_title_list[i]; })
   var trs    = tables.append('tbody').selectAll('tr')
                   .data(function (d,i) {if (data_dict[d]["procs"]!=undefined) {return Object.values(data_dict[d]["procs"])} else {return []}})
                   .enter().append('tr')
                      .attr('class','noborder')
   trs.selectAll('td')
      .data(function(d) {return idx_list.map(x => d.at(x))})
      .enter().append('td')
         .attr('class', function(d,i) {return 'noborder ' + table_title_list[i]})
         .html(function (d, i) {
              return getTypedValueHtml(d, type_list[i], cluster);
         })

   // collapse behavior
   collapseTitle ()
} //createJobProc

//data_dict is a dictionary of a fixed format
function createMultiTable2 (data_dict, parent_id, table_title_list, node, type_list) {
   console.log("createMultiTable2 data_dict=", data_dict, "table_title_list=", table_title_list, "node=", node, "type_list=", type_list)
   var pas    = createMultiTitle2(data_dict, parent_id, node)
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

   // collapse behavior
   collapseTitle ()
} //createMultiTable2

//titles_dict define display fields
function getSummaryTRHtml (d, titles_dict, summary_type, sortCol) {
   var tds = Object.keys(titles_dict).reduce(function(str, key) {
                                      var td_html = getSummaryHtml(d, key, summary_type, sortCol)
                                      if (key == getExpandCol(sortCol))
                                         return str + '<td class="expand">' + td_html + '</td>';
                                      else
                                         return str + '<td>' + td_html + '</td>'
                                   },'')
   //console.log("getSummaryTRHtml", tds)
   return tds
}
function getSummaryHtml (d, key, summary_type, sortCol)
{
   if (d[key] == undefined) return ''
   if (key == sortCol) {
      if (summary_type[key] == 'avg') return d[key].toFixed(2);   // coordinate with equalOnCol
      else                            return d[key]
   } else if (summary_type[key]) {
      if (summary_type[key] == 'avg')
         if (Summary_TYPE_DICT[key] == 'Byte') return 'Avg ' + getDisplayFloat(d[key])+'B';
         else                                  return 'Avg ' + getDisplayFloat(d[key]);
      else if (summary_type[key] == 'total')
         return 'Total ' + d[key];
      else
         return d[key]
   } else {  // default set
      const firstVal = d[key].values().next().value
      if (d[key].size == 1)
         return firstVal;
      else
         return 'Count ' + d[key].size + ' [' + firstVal + ',...]';
   }
}

DISPLAY_FUNC={'user': getUserDetailHtml, 
              "timestamp":getTS_LString, 'float':getDisplayF, 'noneg_float':getDisplayNonNegF, "M":getDisplayM,"bigInt":getDisplayI,
              'partition':getPartDetailHtml,'partition_list_avail':getPartListAvailString,"partition_list": getPartitionListHtml, 
              'qos':getQoSDetailHtml, 
              'job':getJobDetailHtml,'job_name':getJobNameHtml,'job_step':getJob_StepHtml,'job_list':getJobListHtml,'job_list_summary':getJobListSummaryHtml,
              'node':getNodeDetailHtml,
              'tres':getTresDisplay, 'tres_usage':getTresUsageString, 
              'gpu_type': getGRESType,
              'file_usage':getFileUsageString,
              'info_alarm':getInfoAlarmHtml,
              'percent2str':percent2str,
              'period':getPeriodDisplay,'period_min':getPeriodMinuteDisplay,
              'cpu_eff':getCPUEffDisplay, 'mem_eff':getMemEffDisplay,
              'job_command':getDisplayFile}

function getTypedValueHtml (d_value, d_type, cluster)
{
   if (d_value == undefined)
      return '';
   var func = DISPLAY_FUNC[d_type]
   if (func) {
      if (["job","job_list", "job_list_summary", "job_step", "job_command", "node", 'partition', 'partition_list', 'qos'].includes(d_type)) {
         return func(d_value, cluster);
      }
      else {     
         return func(d_value);
      }
   }

   if (d_type) {
      if (d_type == 'TresShort')
                       return getTres_short(d_value)
      else if (d_type == 'JobArray' && d_value)
                       return getJobArrayDetail(d_value)
      else if (d_type == 'JobName')
                       return '<a href=./jobByName?name=' + d_value+'>' + d_value + '</a>'
      else if (d_type == 'Node')
                       return getNodeDetailHtml (d_value)
      else if (d_type == 'User')
                       return '<a href=./userDetails?user=' + d_value+'>' + d_value + '</a>'
      else if (d_type == 'Float')
           return getDisplayFloat(d_value)
      else if (d_type == 'BigInt')
           return getDisplayI(d_value)
      else if (d_type == 'Byte')
           return getDisplayI(d_value)+'B'
      else if (d_type == 'Bps')
           return getDisplayBps(d_value)
   }
   return d_value;
}

//for summary table
function getAlarmClass (d, key, alarm_list) {
   var classes = alarm_list.flatMap(function(alarm) {
                    if (key in alarm && (d[key] != undefined)) {
                       var val = d[key]
                       if        (key.includes("rss") && ('node_mem_M' in d) ) {
                          val = d[key]/1024/1024/d.node_mem_M*100
                       } else if (key.includes("cpu") && ('alloc_cpus' in d) ) {
                          val = d[key]*100/d.alloc_cpus
                       } else if (key.includes("gpu") && ('alloc_gpus' in d) )
                          val = d[key]*100/d.alloc_gpus
                       if ( (alarm.type == "alarm" && val>alarm[key]) || (alarm.type == "inform" && val<alarm[key]))
                          return alarm.type
                    }
                    return []
                  })
   return classes
}

//titles_dict define display fields
function getTRHtml (d, titles_dict, type_dict, cluster) {
   var tds = Object.keys(titles_dict).reduce(function(str, key) {
                                      var td_html = getTypedValueHtml(d[key], type_dict[key], cluster)
                                      var classes = getAlarmClass (d, key, Summary_ALARM)
                                      if (type_dict[key] == 'Float' || type_dict[key] == 'BigInt' || type_dict[key] == 'Int')
                                         classes.push("right")
                                      if (classes.length>0)
                                         return str + '<td class="' + classes.join(' ')  + '">' + td_html + '</td>';
                                      else
                                         return str + '<td>' + td_html + '</td>'
                                   },'')
   return tds
}

var Summary_ALARM    =null      // a list
var Summary_TYPE_DICT=null      // a dict
function createSummaryTable (data, titles_dict, table_id, parent_id, type_dict, summary_type, alarm_lst, cluster=DEF_CLUSTER) {
   console.log("createSummaryTable data=", data, ",alarm_lst=", alarm_lst, ",cluster=", cluster)
   Summary_ALARM     = alarm_lst
   Summary_TYPE_DICT = type_dict
   data.forEach (function(d) { d.html = getTRHtml (d, titles_dict, type_dict, cluster) });
   var table = d3.select('#'+parent_id).append('table')
                                          .property('id', table_id)
                                          .attr('class', 'slurminfo');
   var tbody = table.append('tbody')
   var allData = sortSummaryTable   (tbody, data, 'status', true, titles_dict, type_dict, summary_type)
   createSummaryTbody (tbody, allData, 'status', titles_dict, type_dict, summary_type)
   createSummaryThead (table, titles_dict, tbody, data, type_dict, summary_type)
   // sort by status in increasing order
   setSummaryTableBehavior ('status', true)
}

function reorderSummaryTable (tbody, data, sortCol, ascOrder, titles_dict, type_dict, summary_type, preSortCol, preAscOrder) {
   var allData = sortSummaryTable   (tbody, data, sortCol, ascOrder, titles_dict, type_dict, summary_type)
   createSummaryTbody (tbody, allData, sortCol, titles_dict, type_dict, summary_type)
   setSummaryTableBehavior (sortCol, ascOrder)
}

function setSummaryTableBehavior (sortCol, ascOrder) {
   // thead's behavior
   $("thead th").removeClass("sort sort_des")
   if (ascOrder)
      $(`thead #${sortCol}`).addClass("sort");
   else
      $(`thead #${sortCol}`).addClass("sort_des");

   // tbody's behavior
   $("tbody tr.detail").hide()                        // hide all summary_convered details
   $("tbody td.summary").click(function(event) {
                           var group = $(this).parent().attr('data-group')
                           var count = $(this).parent().attr('data-group-cnt')
                           $(this).toggleClass('collapse')  //toggleClass() toggles between adding and removing one or more class names from the selected elements.
                           $(this).parent().nextAll(`:lt(${count})`).toggle()
                       });
   $("tbody tr.summary").click(function(event) {
                           var group = $(this).attr('data-group')
                           var count = $(this).attr('data-group-cnt')
                           $(this).nextAll(`:lt(${count})`).toggle()
                       });
}

function sortSummaryTable (tbody, data, sortCol, ascOrder, titles_dict, type_dict, summary_type) {
   // Note: simple reverse data order does not work as the order of tiebreaker is not right
   sortDataOnCol (data, sortCol, ascOrder, getExpandCol(sortCol))
   var summaryData  = getSummaryData (data, sortCol, titles_dict, summary_type) // summary.start=data.length- 1-summary.start should work if sortCol==preSortCol TODO: improve performance
   var allData      = merge(data, summaryData, sortCol)
   //console.log("sortSummaryTable allData=", allData, ',data=', data, 'summary=', summaryData)

   return allData
}
//summary tbody tr has class summary, detail

function createSummaryTbody (tbody, allData, sortCol, titles_dict, type_dict, summary_type) {
   var currSummary = null
   var idxSummary  = null
   var expCol      = getExpandCol(sortCol)
   tbody.selectAll('*').remove();
   //console.log("create tr", Date.now())
   var rows = tbody.selectAll('tr').data(allData).enter()
                   .append('tr')
                      .attr('data-group',     function(d) {return d[sortCol]})
                      .attr('data-group-cnt', function(d) {return d.count})
                      .attr('class', function(d, i) {
                                        if (d.type=='summary') {
                                           currSummary =d;
                                           idxSummary  =i;
                                           return 'summary';
                                        } else {
                                           if (currSummary && i < idxSummary+1+currSummary.count)
                                              return currSummary[sortCol] + ' detail';  // needed to hide all details
                                        }
                            })
                      .html(function(d) {
                               return d.html});
   //use d3 to generate rows are slow
   // set up behavor
   // sortCol of summary row has class "expand", on click triggle display detail rows, then toggle class
   //
};

function merge (detailData, summaryData, sortCol) {
   var data      = []
   var d_idx     = 0
   const notEmptySortCol = ['node', 'status', 'job', 'user', 'alloc_cpus', 'alloc_gpus', 'gpu_util', 'avg_gpu_util']
   var check     = false
   if (notEmptySortCol.includes(sortCol)) check = true
   for (var s_idx=0; s_idx < summaryData.length; s_idx++) {
       currS = summaryData[s_idx]

       for (var i=d_idx; i<currS.start; i++) {
           if (!check)
              data.push(detailData[d_idx]);
           else if (check && detailData[d_idx][sortCol] != undefined)
              data.push(detailData[d_idx]);
           d_idx = d_idx + 1
       }
       if (!check || (check && currS[sortCol] != undefined))
          data.push(currS)

   }
   while (d_idx < detailData.length ) {
       if (!check || (check && currS[sortCol] != undefined))
          data.push(detailData[d_idx])
       d_idx  = d_idx + 1
   }
   //console.log("merge ", data.length)
   return data
}

function sortFun_inc (a,b) {
   if (a[d] == b[d]) {
      if (b[firstKey]<a[firstKey]) {return 1;} else {return -1;}}
   else {
      if (b[d] < a[d]) {return 1;} else {return -1;} }
}

//sort data on column key in asc(true/false) order
function sortDataOnCol (data, sortCol, asc, tiebreak) {
   data.sort (function (a, b) {
      if (asc) return incFunc(a, b, sortCol, tiebreak)
      else     return decFunc(a, b, sortCol, tiebreak)
   })
}
function incFunc (a, b, col, tiebreak) {
   var cmp = incF (a, b, col)
   if ( cmp!=0 )
      return cmp
   else
      return incF (a, b, tiebreak)
}
function decFunc (a, b, col, tiebreak) {
   var cmp = -incF (a, b, col)
   if ( cmp!=0 )
      return cmp
   else
      return incF (a, b, tiebreak)         //tiebreak keep inc order
}
function incF (a, b, col) {
   if ( !a[col] && !b[col] )   return 0;   //both are undefined, 0 or null
   if ( !a[col] || !b[col] )  {          // one of it is undefined, 0, or null
      if ( a[col] )            return 1;
      else                     return -1;
   }
   if ( a[col] == b[col] )     return 0;
   else if ( a[col] > b[col] ) return 1;
   else                        return -1;

   return 0;
}
//summary sIdx over the same load_data[idx] value
////format [[startRow, endRow, record], ...]
function getSummaryData(table_data, sortCol, titles_dict, summary_type ) {
   var result = []

   // loop over the table_data
   var i      = 0
   while (i < table_data.length) {
      var curr = table_data[i]
      var j=i
      //loop to next different value, summarize information during loop
      while ( (j<table_data.length-1) && equalOnCol(curr, table_data[j+1], sortCol, summary_type) ) j++;  //table_data[j]==table_data[i]
      if (j>i) {
         //save summary information
         summaryRecord       = getSingleSummaryData(table_data, i, j, sortCol, titles_dict, summary_type)
         summaryRecord.start = i
         summaryRecord.count = j-i+1
         summaryRecord.type  = 'summary'
         summaryRecord.html  = getSummaryTRHtml (summaryRecord, titles_dict, summary_type, sortCol)
         result.push(summaryRecord)
      }
      i=j+1
   }
   console.log('getSummaryData', result)
   return result
}

//summary {'node':[values ...], ...}
//return a summary record that summarize rows startRow - endRow(included), these rows have some value on sortCol
function getSingleSummaryData(table_data, startRow, endRow, sortCol, titles_dict, summary_type) {
   var currRow     = startRow
   var summary     = Object.keys(titles_dict).reduce(function(rltObj, key) {rltObj[key]=[];return rltObj}, {})
   // loop over row to get data
   while (currRow <= endRow) {
      // loop over the col to get data
      for (const key of Object.keys(titles_dict)) {
          var value = table_data[currRow][key]
          if (value != undefined)
             summary[key].push(value)
      }
      currRow ++;
   }
   // deal with data
   const count = endRow - startRow + 1
   for (var key of Object.keys(summary)) {
       if (summary[key].length == 0 )
          summary[key] = undefined
       else if (key == sortCol ) {
          summary[key] = summary[key][0]      // should have only one value
       } else if (summary_type[key] == 'total' ) {
          summary[key] = summary[key].reduce(function (sum, curr) {return sum + curr;}, 0)
       } else if (summary_type[key] == 'avg' ) {
          var total = summary[key].reduce(function (sum, curr) {return sum + curr;}, 0)
          summary[key] = total / count
       } else {
          summary[key] = new Set(summary[key])
       }
   }
   return summary
}
//
// whether two row are equal on col
function equalOnCol (r1, r2, col, summary_type) {
   if ((summary_type[col] == 'avg') && r1[col] && r2[col]) {
      //console.log ("comp ", r1, r2, ",", r1[col].toFixed(2), "-", r2[col].toFixed(2))
      return r1[col].toFixed(2) == r2[col].toFixed(2);   //coordinate with getSummaryHtml
   } else
      return r1[col] == r2[col];
}

function createSummaryThead (table, titles_dict, tbody, data, type_dict, summary_type) {
   var sortAscending = Object.keys(titles_dict).reduce(function(obj, curr) {obj[curr]=true; return obj;}, {})
   var currSortCol   = "status"
   sortAscending['status'] = false
   var firstKey      = Object.keys(titles_dict)[0]   <!--use jobid as tie breaker for sorting -->
   var thead         = table.append('thead')
   var th = thead.append('tr').selectAll('th')
                      .data(Object.keys(titles_dict)).enter()
                      .append('th')
                      .attr('id', function(d) {return d})
                      .html(function (d) { return titles_dict[d] })
                      .on('click', function (d) {  // every td can be sorted
                         //console.log('---', d, ',' ,sortAscending[d])
                         reorderSummaryTable (tbody, data, d, sortAscending[d], titles_dict, type_dict, summary_type, currSortCol, !sortAscending[d])
                         //tbody.selectAll('tr').lower() //each(function() { console.log(this); this.parentNode.appendChild(this); });
                         currSortCol      = d
                         sortAscending[d] = !sortAscending[d]
                      });
    return thead;
}

function createSortTableHeader_1 (table, title_list, idx_list, rows_list) {
   var sortAscending = true;
   var tieKey        = title_list[0]   <!--use procid as tie breaker for sorting -->
   var headers = table.append('thead').append('tr').selectAll('th')
                      .data(title_list).enter()
                      .append('th')
                      .attr('id', function(d) {return d})
                      .text(function (d) { return d; })
                      .on('click', function (d_txt, i) {
			       var d       = idx_list[i];
			       console.log("click " , d, ":", d_txt)
                               headers.attr('class', 'header');   //set all th's class
                               // must return 0, 1, -1, return true/false will not work
                               rows_list.sort(function(a, b) {
			          var asc_cmp = 1;
			          if (a.at(d) > b.at(d))
				     asc_cmp = -1
			          else if (a.at(d) == b.at(d)) {
                                     if (a.at(tieKey)>b.at(tieKey)) 
				        asc_cmp = -1
			          } 
				  console.log("a=", a.at(d), ", b=", b.at(d))
				  if (sortAscending)
				       return asc_cmp
				  else
				       return -asc_cmp;
			       })
                               if (sortAscending) {
                                  sortAscending  = false;
                                  this.className = 'aes';
                               } else {
                                  sortAscending  = true;
                                  this.className = 'des';
                               }
                      });
    return headers;
}
function createTableHeader (table, titles_dict, rows) {
   var sortAscending = true;
   var firstKey      = Object.keys(titles_dict)[0]   <!--use jobid as tie breaker for sorting -->
   var headers = table.append('thead').append('tr').selectAll('th')
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
                                         if (b[firstKey]<a[firstKey]) {return 1;} else {return -1;}} // firstKey always ascending
                                      else {
                                         if (b[d] < a[d]) {return -1;} else {return 1;} } });
                                  sortAscending  = true;
                                  this.className = 'des';
                               }
                      });
    return headers;
}

//data is a list of dict
function createTable (data, titles_dict, table_id, parent_id, pre_data_func, type_dict, cluster=DEF_CLUSTER) {
        console.log("createTable ", table_id, " data=", data, " titles_dict=", titles_dict, ",type_dict=", type_dict)
        var table         = d3.select('#'+parent_id).append('table').property('id', table_id);

        if (pre_data_func != undefined)
           pre_data_func (data)   //data is modified on site
        var rows = table.append('tbody').selectAll('tr')
                               .data(data).enter()
                               .append('tr')
                               .attr('data-group', function(d) {
                                  if ( d.data_group) return d.data_group ;})
                               .attr('data-group-idx', function(d) {
                                  if ( d.data_group) return d.data_group_idx;})
                               .attr('data-group-cnt', function(d) {
                                  if ( d.data_group) return d.data_group_cnt;})
                               .attr('class', function(d) {
                                  if ( d.display_class) return d.display_class;})

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
                 if (type_dict) {
                    d_type = type_dict[d.name]
                    return getTypedValueHtml(d.value, d_type, cluster);
                 }
                 return d.value;
            });
        createTableHeader(table, titles_dict, rows)
};
function getTres_short(tres) {
    return tres.replace(/,billing=\d+/,'')
}

function prepareData_pending (data) {
   var savGID   = 'noGroup'
   var savGName = 'noGroup'
   var savGroup = []
   var group

   data.forEach (function (d) {
      group = d.user+d.partition+d.qos;   // will collapse onto same group in the pending table
                                          // identical user ID, partition name, account, and QOS -> identical user ID, parition name
      group = group.replace(",", "_");            // fix the bug with multiple partition request
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

const SUMMARY_EXPAND_COL={'status':'node', 'delay':'node', 'job':'node'}    // default is job
function getExpandCol (sortCol) {
   var expCol      = SUMMARY_EXPAND_COL[sortCol]
   if (expCol) return expCol
   else        return "job"
}

//q3 data binding need to arrays
////return [[key, value],...]
//
function filterDict2NestList (data_dict, req_fields)
{
    if (!req_fields)   // empty req_fields will return all fields
       return Object.entries(data_dict)   //TODO:
    var f      = req_fields.filter(function (k) {return data_dict.hasOwnProperty(k) && data_dict[k] && data_dict[k]!="None" && data_dict[k].toString()!=''})  //keep the order in fields
    return f.map(function (k) { return [k, data_dict[k]] })
}
//table without header, 1st column is key, 2nd column is value
//input: data_dict is a dictionary with {key: value}, 
//       disp_dict is a dictionary {display_key: display_text}, 
//       type_dict is {key: display_type}
function createNoHeaderTable (data_dict, disp_dict, type_dict, parent_id, table_id, prepare_data_func=filterDict2NestList, cluster=DEF_CLUSTER){
    console.log('createNoHeaderTable: data_dict=', data_dict, ',disp_dict=', disp_dict, ",type_dict=", type_dict, ",cluster=", cluster)
    var data  = prepare_data_func(data_dict, Object.keys(disp_dict))
    var table = d3.select(parent_id).append('table').property('id', table_id)
                                                    .attr('class', 'no_header');
    var trs   = table.append('tbody').selectAll('tr')
                                     .data(data).enter()
                                     .append('tr')
    var tds   = trs.selectAll('td')
                   .data(function (d, i) {
                           return [d[0], {'key':d[0], 'value':d[1]}];
                        }).enter()
                   .append('td')
                   .html(function (d, i) {
                        if (i==0)                     // Attribute Name
                           return disp_dict[d];
                        if (i==1) {                   // Attribute Value
                           var d_type = type_dict[d.key]
                           return getTypedValueHtml (d.value, d_type, cluster)
                        }
                        return d; })
}

function createJobHistoryTable (job_history, array_het_jids, job_history_table_id, parent_id, excludeGPU=false, excludeEff=false) {
   if (array_het_jids.length)
      tmp = {'JobIDRaw':'Job ID', 'JobID':'Job ID(Report)'}
   else 
      tmp = {'JobID':'Job ID'}
   const j_h_titles = Object.assign(tmp, {'JobName':'Job Name','State':'State','NodeList':'Alloc Node','AllocCPUS':'Alloc CPU','AllocGPUS':'Alloc GPU','Start':'Start Time','End':'End Time','Job Wall-clock time':'Wall-clock time','CPU Efficiency':'CPU Efficiency','Memory Efficiency':'Memory Efficiency'})
   if (excludeGPU)
      delete j_h_titles.AllocGPUS
   if (excludeEff)
      delete j_h_titles['Job Wall-clock time']
      delete j_h_titles['CPU Efficiency']
      delete j_h_titles['Memory Efficiency']
   createTable (job_history, j_h_titles,  job_history_table_id, parent_id, undefined, {'JobID':'job_step', 'JobIDRaw':'job_step','JobName':'job_name'})
}

function createUserJobHistoryTable (job_history, array_het_jids, job_history_table_id, parent_id, excludeGPU=false) {
   if (array_het_jids.length)
      tmp = {'JobIDRaw':'Job ID', 'JobID':'Job ID(Report)'}
   else 
      tmp = {'JobID':'Job ID'}
   const j_h_titles = Object.assign(tmp, {'JobName':'Job Name','State':'State','NodeList':'Alloc Node','AllocCPUS':'Alloc CPU','AllocGPUS':'Alloc GPU','Start':'Start Time','End':'End Time','wall_clock':'Wall-clock time','cpu_eff':'CPU Efficiency','mem_eff':'Memory Efficiency'})
   if (excludeGPU)
      delete j_h_titles.AllocGPUS
   createTable (job_history, j_h_titles,  job_history_table_id, parent_id, undefined, {'JobID':'job_step', 'JobIDRaw':'job_step','JobName':'job_name','wall_clock':'period', 'cpu_eff':'cpu_eff', 'mem_eff':'mem_eff'})
}

function createUserPartTable (part_info, part_table_id, parent_id, cluster=DEF_CLUSTER) {
   const part_titles    = {'name': 'Part.', 'flag_shared':'Sharable',
                           'user_avail_nodes':'Node', 'user_avail_cpus':'CPU', 'user_avail_gpus': 'GPU',
                           'user_lmt_nodes'  :'Node', 'user_lmt_cpus'  :'CPU', 'user_lmt_gpus'  :'GPU',
                           'user_alloc_nodes':'Node', 'user_alloc_cpus':'CPU', 'user_alloc_gpus':'GPU',
                           'grp_avail_nodes' :'Node', 'grp_avail_cpus' :'CPU', 'grp_avail_gpus' :'GPU',
                           'grp_lmt_nodes'   :'Node', 'grp_lmt_cpus'   :'CPU', 'grp_lmt_gpus'   :'GPU',
                           'avail_nodes'     :'Node', 'avail_cpus'     :'CPU', 'avail_gpus'     :'GPU',
                          }

   createTable (part_info, part_titles, part_table_id, parent_id, undefined, {'name':'partition'}, cluster)
   // add one extra line to category table headers
   $('#' + part_table_id + ' thead').prepend('<tr><th colspan="2"></th><th colspan="3">User Avail</th><th colspan="3">User QoS Lmt</th><th colspan="3">User Alloc</th><th colspan="3">Grp Avail</th><th colspan="3">Grp QoS Lmt</th><th colspan="3">Part. Avail</th></tr>')
   $('#' + part_table_id + ' td[data-th^="user_avail_"]').addClass("inform")
}
