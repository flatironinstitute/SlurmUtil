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
   if (n < 1024) 
      return n.toFixed(2)
   else {
      n /= 1024
      return getDisplayK(n)
   }
}

function createMultiTitle (data_dict, parent_id, job_id) {
   var pas = d3.select(parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick table_title')
      .html(function (d) {
               var procLink = '<a href="./nodeJobProcGraph?node=' + d + '&jid=' + job_id + '"> (Proc Usage Graph) </a>'
               var rlt      = '<a href="./nodeDetails?node=' + d + '">' + d + '</a>: ' + data_dict[d][1] + ' processes'
               if (data_dict[d][0]>0)
                  rlt  = rlt + ' on ' + data_dict[d][0] + ' CPUs'
               rlt          = rlt + procLink
               return rlt})
   return pas
}

//data_dict is a dictionary of a fixed format
function createMultiTable (data_dict, parent_id, table_title_list, job_id) {
   console.log("createMultiTable: data_dict=", data_dict, ", table_title_list=", table_title_list)
   var pas    = createMultiTitle (data_dict, parent_id, job_id)
   var tables = pas.append('table').property('id', function(d) {return d+'_proc'}).attr('class','noborder')
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
}

function createMultiTitle2 (data_dict, parent_id, node) {
   var pas = d3.select(parent_id).selectAll('p')
               .data(Object.keys(data_dict))
               .enter().append('div')
   pas.append('p')
      .attr('class', 'thick table_title')
      .html(function (d) {
               var str= "Job " + d + ': '
               if ( d!= 'undefined') {
                  var job = data_dict[d]["job"]
                  str += 'alloc ' + job["cpus_allocated"][node] + ' CPUs'
                  if (("gpus_allocated" in job) && (node in job["gpus_allocated"]))  
                     str += ', ' + job["gpus_allocated"][node].length + ' GPUs'
                  if (data_dict[d]["procs"] != undefined)
		     str += ', running processes ' + data_dict[d]["procs"].length +'<a href="./nodeJobProcGraph?node=' + node + '&jid=' + d + '"> (Proc Usage Graph) </a>'
                  else
		     str += ', no running processes.'
               }
               return str; })
    return pas
}

function collapseTitle () {
   // collapse behavior
   $('.table_title').click(function(event) {
       //$(this).children("i").toggleClass('hide')
      $(this).toggleClass('table_title_collapse')
      $(this).next().toggleClass('hide')
   });
}

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
}

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

function getTypedHtml (d, type_dict)
{
   if (d.value && type_dict)
      return getTypedValueHtml (d.value, type_dict[d.name]);
   else
      return d.value;
}

function getTypedValueHtml (d_value, d_type)
{
   if (d_value == undefined)
      return '';
   if (d_type) {
      if (d_type == 'Partition')
                       return getPartDetailHtml(d_value)
      else if (d_type == 'Time')
                       return getTS_string(d_value)
      else if (d_type == 'TresShort')
                       return getTres_short(d_value)
      else if (d_type == 'JobList') {
                       var jids = d_value.split(" ")
                       var str  = ''
                       for (jid of jids) 
                           str  = str + ' ' + getJobDetailHtml(jid)
                       return str }
      else if (d_type == 'JobArray' && d_value) 
                       return getJobArrayDetail(d_value)
      else if (d_type == 'JobName') 
                       return '<a href=./jobByName?name=' + d_value+'>' + d_value + '</a>'
      else if (d_type == 'JobAndStep') {
                       if ((d_value.toString().indexOf('.') == -1) && (d_value.toString().indexOf('_')==-1))   // not jobstep
                          return '<a href=./jobDetails?jid=' + d_value+'>' + d_value + '</a>'
                          return '<a href=./jobDetails?jid=' + d_value+'>' + d_value + '</a>'}
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
/*
                       if (alarm.type == "alarm") {
                          if (key.startsWith("rss")) {
                             if (('node_mem_M' in d) && (d[key]/1024/1024/d.node_mem_M*100>alarm[key])) {
                                return alarm.type;
                             }
                          } else if (key.includes("cpu") && ('alloc_cpus' in d) && (d[key]*100/d.alloc_cpus>alarm[key])) {
                                return alarm.type;
                          } else if (key.includes("gpu") && ('alloc_gpus' in d) && (d[key]*100/d.alloc_gpus>alarm[key]))
                                return alarm.type;
                       } else if (alarm.type == "inform") {
                          if (key.startsWith("rss")) {
                             if (('node_mem_M' in d) && (d[key]/1024/1024/d.node_mem_M*100<alarm[key]))
                                return alarm.type;
                          } else if (key.includes("cpu") && ('alloc_cpus' in d) && (d[key]*100/d.alloc_cpus<alarm[key])) {
                                return alarm.type;
                          } else if (key.includes("gpu") && ('alloc_gpus' in d) && (d[key]*100/d.alloc_gpus<alarm[key]))
                                return alarm.type;
                       }  // else return []
                    }        
                    return []
                 }) 
*/
   return classes
}

//titles_dict define display fields
function getTRHtml (d, titles_dict, type_dict) {
   var tds = Object.keys(titles_dict).reduce(function(str, key) {
                                      var td_html = getTypedValueHtml(d[key], type_dict[key])
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
function createSummaryTable (data, titles_dict, table_id, parent_id, type_dict, summary_type, alarm_lst) {
   console.log("createTable data=", data, ",titles=", titles_dict, ",type=", type_dict, ",alarm=", alarm_lst)
   Summary_ALARM     = alarm_lst
   Summary_TYPE_DICT = type_dict   
   data.forEach (function(d) { d.html = getTRHtml (d, titles_dict, type_dict) });
   var table = d3.select('#'+parent_id).append('table')
                                          .property('id', table_id)
                                          .attr('class', 'slurminfo');
   var tbody = table.append('tbody')
   var allData = sortSummaryTable   (tbody, data, 'status', true, titles_dict, type_dict, summary_type)
   createSummaryTbody (tbody, allData, 'status', titles_dict, type_dict, summary_type)
   createSummaryThead (table, titles_dict, tbody, data, type_dict, summary_type)          
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
   //console.log(rows)
   //console.log("create td", Date.now())
/*
   rows.selectAll('td')
       .data(function (d) {
                // if d summary or detail
                if (d.type=='summary')
                   return Object.keys(titles_dict).map(function (k) {
                                                       if (d[k]!=undefined) return { 'value': getSummaryHtml(d,k,summary_type,sortCol), 'name': k, 'type':'summary'};
                                                       else      return { 'value': '',   'name': k};
                                                    });
                else
                   return Object.keys(titles_dict).map(function (k) {
                                                       if (d[k]!=undefined) return { 'value': d[k], 'name': k};
                                                       else      return { 'value': '',   'name': k};
                                                    });
       }).enter()
       .append('td')
            .attr('class', function(d) { if (d.type=='summary' && d.name==expCol) return "expand";
                                         else                                     return type_dict[d.name];}) 
            .html(         function(d) { if (d.type=='summary') return d.value;
                                         //else                   return getTypedHtml(d, type_dict) });
                                         else                   return d.value });
*/
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
                               console.log("click rows=", rows.datum())
                      });
    return headers;                  
}

//data is a list of dict
function createTable (data, titles_dict, table_id, parent_id, pre_data_func=prepareData, type_dict) {
        console.log("createTable data=", data, ",pre_data_fun=", pre_data_func, ",type_dict=", type_dict)
        var table         = d3.select('#'+parent_id).append('table').property('id', table_id);

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
                 return getTypedHtml (d, type_dict)
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
      //group = d.user+d.partition+d.state_reason;   // will collapse onto same group in the pending table
                                                   // identical user ID, partition name, account, and QOS -> identical user ID, parition name
      group = d.user+d.partition+d.qos;   // will collapse onto same group in the pending table
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

