//typeof(data) is object and thus pass by reference
function prepareData (data) {
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

   console.log (data)
   return data
}

function createTable (data, titles_dict, table_id, parent_id) {
        var sortAscending = true;
        var table         = d3.select('#'+parent_id).append('table').property('id', table_id);
        var firstKey      = Object.keys(titles_dict)[0]   <!--use jobid as tie breaker for sorting -->

        prepareData (data)
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
                 }
                
                 return d.value;
            });
};

