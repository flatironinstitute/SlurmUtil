//typeof(data) is object and thus pass by reference
const     color_cnt   = 10
          colors      = ["#0d0dff", "#0d97ff", "#0dffff", "#0dff87","#0dff0d","#87ff0d", "#ffff0d", "#ff870d","#ff5b5b", "red"], 
          
          accounts    = ['cca', 'ccb', 'ccm', 'ccn', 'ccq', 'etc'],
          redColor    = ["#fff5f5","#ffd0d0","#ffb6b6","#ff9c9c","#ff8282","#ff6868","#ff4e4e","#ff2727","#f30000","#800000"], 
          purpleColor = ["#f5edfc","#e8d0ff","#dbb6ff","#cf9cff","#c282ff","#b568ff","#a84eff","#9527ff","#7a00f3","#531a88"], 
          greenColor  = ["#eaffea","#d0ffd0","#b6ffb6","#9cff9c","#82ff82","#68ff68","#4eff4e","#27ff27","#00f300","#008f11"],
          orangeColor = ["#fff5ea","#ffe8d0","#ffdbb6","#ffcf9c","#ffbb75","#ffa84e","#ff9527","#f37a00","#cf6800","#ab5600"],
          blueColor   = ["#eaf5ff","#d0e8ff","#b6dbff","#9ccfff","#82c2ff","#68b5ff","#4ea8ff","#2795ff","#007af3","#0068cf"],
          tealColor   = ["#e0ffff","#c2ffff","#94FFFF","#66FFFF","#0BFFFF","#00DCDC","#00AEAE","#008080","#175873","#05445E"],
          acctColors  = {'cca': redColor, 'ccb': greenColor, 'ccm': orangeColor, 'ccn': tealColor, 'ccq': purpleColor, 'etc': blueColor}

var colorScale = d3.scale.quantize()
              .domain([0, colors.length - 1, 1])
              .range(colors);
var colorScales = {}
for (var acct of accounts) {
    colorScales[acct] = d3.scale.quantize()
                 .domain([0, colors.length - 1, 1])
                 .range(acctColors[acct]);
}

function createLegend (svg_id, width, height, acctColor, gridSize) {
    var legend_svg  = d3.select(svg_id)
          .attr("width",  width)
          .attr("height", height)
          .append("g")
             .attr("transform", "translate(" + 120 + "," + 10 + ")");

    var y=0
    var legend = legend_svg.selectAll(".legend")
                 .data([-0.2,-0.1,0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]);
    if (acctColor) {
            for (var acct of accounts) {
             legend.enter().append("g")
                 .attr("class", "legend");
             legend.append("rect")
               .attr("x", function(d, i) { return gridSize * 2 * i; })
               .attr("y", y)
               .attr("width",  gridSize * 2)
               .attr("height", gridSize / 2)
               .style("fill",  function(d, i) { if (i==0) return "black"; 
                                                else if (i==1) return "gray";
                                                else return acctColors[acct][i-2]; });
             y = y + gridSize/2
             legend.append("text")
               .attr("class", "mono")
               .text(acct)
               .attr("x", -gridSize)
               .attr("y", y);
             }
    } else {
             legend.enter().append("g")
                 .attr("class", "legend");
             legend.append("rect")
               .attr("x", function(d, i) { return gridSize * 2 * i; })
               .attr("y", y)
               .attr("width",  gridSize * 2)
               .attr("height", gridSize / 2)
               .style("fill",  function(d, i) { if (i==0) return "black"; 
                                                else if (i==1) return "gray";
                                                else return colors[i-2]; });
             y = y + gridSize/2
    }
    legend.append("text")
               .attr("class", "mono")
               .text(function(d, i) { if (i==0) return "Down"; 
                                      else if (i==1) return "Idle";
                                      else if (i==2) return "Alloc"; 
                                      else return "≥ " + d; })
               .attr("x", function(d, i) { return gridSize * 2 * i; })
               .attr("y", y+gridSize/2);
    legend.exit().remove();
    return legend_svg;
};

function createHeatMap(svg, data, acctColor, gridSize, colorAttr="util", cluster) 
{
          //svg.selectAll("*:not(text)").remove()
          //svg.selectAll("*").remove()
          var cards = svg.selectAll(".node")
              .data(data);
          cards.enter().append("rect")
              .attr("x", function(d, i) { var x = d.x; return x * gridSize; })
              .attr("y", function(d, i) { var y = d.y; return y * gridSize; }) 
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", function(d) { return d.classes;})
              .attr("width",  gridSize)
              .attr("height", gridSize)
              .attr("title", function (d) {return d["name"]})
              .attr("value", function (d) {return d["util"]})
              .on("dblclick", function(d) {
                              if (d3.event.shiftKey) { // Ctrl seems not working with MacOS
                                 location.href=getJobDetailHref (d["jobs"], cluster)
                              } else {
                                 location.href=getNodeDetailHref(d["name"], cluster)
                              }})
              .on("click", function(d) {
                              $(".popout").removeClass("popout");
                              var keyArr=d["jobs"]; 
                              keyArr.forEach(function(e) {
                                               $(`.${e}`).addClass("popout")});
                              })
              .style("fill",  "gray");
          cards.transition().duration(1000)
              .style("fill", function(d) { var currColorS = getColorScale(d, acctColor)
                                           if (d.name=="worker1006")
                                              console.log ('currColorS=', currColorS, 'd=', d, "return=", currColorS(d[colorAttr]));
                                           if (d["stat"]==0) return "gray";
                                           else if (d["stat"]==-1) return "black";
                                           else return currColorS(d[colorAttr]); });
          cards.append("title");
          cards.select("title").text(function(d) {return d["labl"]});
          cards.exit().remove();
};

function createSelectList (parent_id, data, tag, value_fld, func_onchg){
          console.log('createSelectList, data=', data)
          d3.select(parent_id.concat(" select")).remove();
          var s = d3.select(parent_id).append("select")
                          .attr("id", tag)
                          .on('change', func_onchg)
                          .on('select.editable-select', function(e) {console.log(e)});
          s.selectAll("option")
                 .data(data)
                 .enter().append("option")
                 .text(function(d) { return d["long_label"]})
                 .attr("value",    function(d) {return d[value_fld]})
                 .attr("disabled", function(d) {if (d["disabled"]) return d["disabled"]; else return undefined});
};

function getGroupID (name) {
   var str = name.slice(6,7);  // worker1001
   var i   = parseInt(str)
   if (isNaN(i))
      return name.slice(6,8);
   else
      return i;
}
function getGroupID_popeye (name) {
   var str = name.split('-');   // pcn-1-12  
   var i   = parseInt(str[1]);
   if (isNaN(i))
      return 100;
   else
      return i;
}

////{"Iron":[node_ts, workers, jobs, users, gpu_ts, gpudata]
function prepareNodeData (data, cntPerLine) {
   var rlt = new Object();
   for (const key in data) {
       console.log(key);
       rlt[key]   = prepareNodeData_1 (key, data[key][1], data[key][5], cntPerLine);
   }
   return rlt;
}

GID_FUNC = {"Iron": getGroupID, "Popeye": getGroupID_popeye}
function prepareNodeData_1 (cluster, nodeData, gpu_obj, cntPerLine) {  // nodeData is sorted by 'name
        console.log("prepareNodeData_1 ", cluster, nodeData, gpu_obj)
        if (nodeData.length == 0)
           return NaN;

        var gid_func     = GID_FUNC[cluster]
        var name2idx     = new Object();           // name2idx {'worker1001':0, ...}, used to combine gpu data
        var line_label   = [];                     // label for each line
        var curr_line    = -1;
        var curr_grp     = 'Not_exist';
        var curr_grp_cnt = 0
        for (var i=0; i<nodeData.length; i++) {  // loop over nodeData list, add gID and 
          var obj         = nodeData[i];
          var grp         = gid_func(obj['name']);
          if ( grp != curr_grp ) {                 // start a new group and a new line
             curr_grp     = grp;
             curr_grp_cnt = 0;
             curr_line   += 1;
             line_label[curr_line] = obj['name'] + ",...";
          } else {
             curr_grp_cnt+= 1;
             if ( curr_grp_cnt % cntPerLine == 0 ) {// start a new line
                curr_line+= 1;
                line_label[curr_line] = obj['name'] + ",...";
             }
          }
          obj.x           = curr_grp_cnt % cntPerLine  
          obj.y           = curr_line;
          obj.classes     = obj.jobs.join(' ');

          name2idx[obj['name']] = i;
        }

        // gpu data
        for (let [gpuID, gpuNodes] of Object.entries(gpu_obj)) {
            if (gpuID == "time") continue;
            for ( let [node, util] of Object.entries(gpuNodes) ) {
                if (name2idx[node] != undefined)
                   //console.log(gpuID, gpuNodes, node, name2idx[node], nodeData[name2idx[node]])
                   nodeData[name2idx[node]][gpuID] = util
            }
        }

        return [nodeData, line_label]
};

function prepareGPUData (data) {
   var rlt = new Object();
   for (const key in data) {
       console.log(key);
       rlt[key]   = prepareGPUData_1 (key, data[key][1]);
   }
   return rlt;
}

//gpu_labels follow nodeData's sort
function prepareGPUData_1 (cluster, nodeData)
{
   console.log("prepareGPUData_1 cluster=", cluster)
   var gpu_labels = []
   var gpuData    = [];             // data that will be used later
   var node_idx   = 0;
   for (d of nodeData) {
       if (d['gpuCount']) {
          for (i=0; i<d['gpuCount']; i++) {
              di = Object.assign({}, d)
              di['gpuIdx']   = i
              if (d['gpus'].hasOwnProperty('gpu'+i)) {
                 di['gpuLabel'] = d['gpus']['gpu'+i]['label']
                 di['stat']     = d['gpus']['gpu'+i]['state']
                 job            = d['gpus']['gpu'+i]['job']
                 di.x = node_idx
                 di.y = i
                 if (job != 0 ) 
                    di['jobs']  = [job]
                 else
                    di['jobs']  = []
             
                 gpuData.push(di)
              }
          }
          gpu_labels.push(d['name'])
          node_idx += 1
      } // ignore non-gpu nodes
   }
   return [gpuData, gpu_labels]
};

function getColorScale(d, useAccountColor) { 
    var currColorS = colorScale
    if (useAccountColor ) { // use color of the account
       if ( d["acct"].length == 1 && d["acct"][0] in colorScales)
          currColorS  = colorScales[d["acct"][0]]
       else if ( d["acct"].length == 1 )
          currColorS  = colorScales["etc"]
       else {
          //console.log("d[acct]", d["acct"])
          var i=1;
          for (i=1;i<d["acct"].length;i++) {
              if (d["acct"][0]!=d["acct"][i])
                 break;
          }
          if (i==d["acct"].length && d["acct"][0] in colorScales)
              currColorS  = colorScales[d["acct"][0]]
          else
              currColorS  = colorScales["etc"]
       }
       return currColorS
    } else
       return colorScale
};

function createGPUHeatMap(svg, gpuData, acctColor, gridSize, cluster="Iron") 
{
   console.log("createGPUHeatMap, gpuData=", gpuData);
          var cards = svg.selectAll(".gpu")
              .data(gpuData);
          cards.enter().append("rect")
              .attr("x", function(d, i) { return d.x * gridSize; })
              .attr("y", function(d, i) { return d.y * gridSize; })
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", function(d) { return d["classes"];})
              .attr("width",  gridSize)
              .attr("height", gridSize)
              .attr("title", function (d) {return d["name"]})
              .attr("value", function (d) {return d["gpu"+d["gpuIdx"]]})
              .on("dblclick", function(d) {
                              if (d3,event.ctrlKey) {
                                 location.href=getJobDetailHref(d["jobs"], cluster)
                              } else {
                                 location.href=getNodeDetailHref(d["name"],cluster)
                              }})
              .on("click", function(d) {
                              var keyArr=d["jobs"];
                              console.log ("---keyArr", keyArr)
                              keyArr.forEach(function(e) {
                                               $(".bordered").removeClass("popout");
                                               $(`.${e}`).addClass("popout")});
                           })
              .style("fill",  "gray");
          cards.transition().duration(1000)
              .style("fill", function(d) { var currColorS = getColorScale(d, acctColor);
                                           if (d["stat"]==0) return "gray";
                                           else if (d["stat"]==-1) return "black";
                                           else return currColorS(d["gpu"+d["gpuIdx"]]); });
          cards.append("title");
          cards.select("title").text(function(d) {return d["gpuLabel"]});
          cards.exit().remove();
};
function createSVG (svg_id, label_id, labels, width, height, margin, gridSize) {
      var svg = d3.select(svg_id)
          .attr("width",  width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
          .append("g")
             .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var labels = svg.selectAll (label_id)
          .data (labels)
          .enter().append("text")
             .text(function (d) {return d;})
             .attr("x", 0)
             .attr("y", function (d, i) { return (i)*gridSize;})
            .style("text-anchor", "end")
             .attr("transform", "translate(-6," + gridSize / 1.5 + ")");

      return svg
};
function createGPUSVG (svg_id, node_label_id, gpu_label_id, node_labels, width, height, gpu_margin, gridSize) {
    var svg    = createSVG (svg_id, gpu_label_id, ['gpu0','gpu1','gpu2','gpu3'], width, height, gpu_margin, gridSize)
    var labels = svg.selectAll (node_label_id)
          .data (node_labels)
          .enter().append("text")
             .text(function (d) {return d;})
             .attr("x", function (d, i) { return (i)*gridSize+10;})
             .attr("y", 0)
            .style("text-anchor", "start")
             .attr("transform", function(d,i) {var x=(i)*gridSize+10; return "rotate(-65,"+x+","+0+")"});
   return svg
};

