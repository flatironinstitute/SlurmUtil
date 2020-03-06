//typeof(data) is object and thus pass by reference
const     color_cnt   = 10
          colors      = ["#0d0dff", "#0d97ff", "#0dffff", "#0dff87","0dff0d","87ff0d", "ffff0d", "#ff870d","#ff5b5b", "red"], 
          
          accounts    = ['cca', 'ccb', 'ccm', 'ccq', 'etc'],
          redColor    = ["#ffeaea","#ffd0d0","#ffb6b6","#ff9c9c","#ff8282","#ff6868","#ff4e4e","#ff2727","#f30000","#cf0000"], 
          purpleColor = ["#f5eaff","#e8d0ff","#dbb6ff","#cf9cff","#c282ff","#b568ff","#a84eff","#9527ff","#7a00f3","#6200cf"], 
          greenColor  = ["#eaffea","#d0ffd0","#b6ffb6","#9cff9c","#82ff82","#68ff68","#4eff4e","#27ff27","#00f300","#00cf00"],
          orangeColor = ["#fff5ea","#ffe8d0","#ffdbb6","#ffcf9c","#ffbb75","#ffa84e","#ff9527","#f37a00","#cf6800","#ab5600"],
          blueColor   = ["#eaf5ff","#d0e8ff","#b6dbff","#9ccfff","#82c2ff","#68b5ff","#4ea8ff","#2795ff","#007af3","#0068cf"],
          acctColors  = {'cca': redColor, 'ccb': greenColor, 'ccm': orangeColor, 'ccq': purpleColor, 'etc': blueColor}

var colorScale = d3.scale.quantize()
              .domain([0, colors.length - 1, 1])
              .range(colors);
var colorScales = {}
for (var acct of accounts) {
    colorScales[acct] = d3.scale.quantize()
                 .domain([0, colors.length - 1, 1])
                 .range(acctColors[acct]);
}

function createLegend (legend_svg, acctColor, gridSize) {
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
};

//
function createHeatMap(svg, data, acctColor, grpCnt, cntLine, gridSize) 
{
          var cards = svg.selectAll(".node")
              .data(data);
          cards.enter().append("rect")
              .attr("x", function(d, i) { var x = d["gIdx"] - Math.floor(d["gIdx"]/cntLine) * cntLine; return x * gridSize; })
              .attr("y", function(d, i) { 
                            var y=0,i; 
                            for (i=0; i<d["gID"]; i++) 
                                y += Math.ceil(grpCnt[i]/cntLine); 
                            return (y + Math.floor(d["gIdx"]/cntLine) )* gridSize; })
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", function(d) { 
                                var str="bordered"; 
                                d["jobs"].forEach(function(e) {str += " " + e}); 
                                return str;})
              .attr("width",  gridSize)
              .attr("height", gridSize)
              .attr("title", function (d) {return d["name"]})
              .attr("value", function (d) {return d["util"]})
              .on("dblclick", function(d) {
                              if (d3,event.ctrlKey) {
                                 location.href="jobDetails?jid="+d["jobs"]
                              } else {
                                 location.href="nodeDetails?node="+d["name"]
                              }})
              .on("click", function(d) {
                              var keyArr=d["jobs"]; 
                              keyArr.forEach(function(e) {
                                               $(".bordered").removeClass("popout");
                                               $(`.${e}`).addClass("popout")})})
              .style("fill",  "gray");
          cards.transition().duration(1000)
              .style("fill", function(d) { var currColorS = getColorScale(d, acctColor)
                                           if (d["stat"]==0) return "gray";
                                           else if (d["stat"]==-1) return "black";
                                           else return currColorS(d["util"]); });
          cards.append("title");
          cards.select("title").text(function(d) {return d["labl"]});
          cards.exit().remove();
};

function createSelectList (parent_id, data, tag, value_fld, func_onchg){
          d3.select(parent_id.concat(" select")).remove();
          var s = d3.select(parent_id).append("select")
                          .on('change', func_onchg)
                          .on('select.editable-select', function(e) {console.log(e)});
          s.selectAll("option")
                 .data(data)
                 .enter().append("option")
                 .text(function(d) { return d["long_label"]})
                 .attr("value",    function(d) {return d[value_fld]})
                 .attr("disabled", function(d) {if (d["disabled"]) return d["disabled"]; else return undefined});
};
function prepareNodeData (nodeData, grpCnt, labels, gpu_obj) {
        var objData  = [];             // data that will be used later

        //nodeData is already sorted by group 
        for (var i=0; i<nodeData.length; i++) {
          var obj = {name: nodeData[i][0], stat: nodeData[i][1], core: nodeData[i][2], 
                     jobs: nodeData[i][4], acct: nodeData[i][5], labl: nodeData[i][6], gpus: nodeData[i][7]};
          if ( obj.stat == 1)
             obj.util=nodeData[i][3]/obj.core;
          else
             obj.util=0
          
          switch (nodeData[i][0].slice(6,7)) {
                 case "0": obj.gID = 0; break; // worker0...
                 case "1": obj.gID = 1; break; // worker1...
                 case "2": obj.gID = 2; break; // worker2...
                 case "3": obj.gID = 3; break; // worker3...
                 case "4": obj.gID = 4; break; // worker4...
                 case "a": obj.gID = 5; break; // workeramd...
                 case "g": obj.gID = 6; obj.gpu=true; break; // workergpu..
                 case "m": obj.gID = 7; break; // workermem..
                 case "p": obj.gID = 8; break; // workerphi..
          }
          obj.gIdx        = grpCnt[obj.gID];
          if ( Math.floor(grpCnt[obj.gID] / cntPerLine)*cntPerLine == grpCnt[obj.gID] )
             labels.push(obj.name+",...");
          grpCnt[obj.gID] ++;
   
          // gpu data
          if (obj['gpu']) {  //TODO: defined by name including 'gpu'
             obj['gpuCount'] =0
             for (gpuID of Object.keys(gpu_obj)) {
                if (gpuID == "time") continue;
                if (gpu_obj[gpuID][obj['name']] != undefined) {
                   obj['gpuCount'] ++
                   obj[gpuID] = gpu_obj[gpuID][obj['name']]
                }
             }
             console.log(obj)
          }
          objData.push (obj);
        }

        return objData
};
//gpu_labels follow nodeData's sort
function prepareGPUData(nodeData, gpu_labels)
{
          var gpuData  = [];             // data that will be used later
          for (d of nodeData) {
             if (d['gpu']) {
                for (i=0; i<d['gpuCount']; i++) {
                   di = Object.assign({}, d)
                   di['gpuIdx'] = i
                   di['gpuLabel'] = d['gpus']['gpu'+i]['label']
                   di['stat']     = d['gpus']['gpu'+i]['state']
                   job            = d['gpus']['gpu'+i]['job']
                   if (job != 0 ) 
                      di['jobs']  = [job]
                   else
                      di['jobs']  = []
                   gpuData.push(di)
                }
                gpu_labels.push(d['name'])
             }
          }
          return gpuData
};

function getColorScale(d, useAccountColor) { 
    var currColorS = colorScale
    if (useAccountColor ) { // use color of the account
       if ( d["acct"].length == 1 && d["acct"][0] in colorScales)
          currColorS  = colorScales[d["acct"][0]]
       else if ( d["acct"].length == 1 )
          currColorS  = colorScales["etc"]
       else {
          var i=1;
          for (i=1;i<d["acct"].length;i++) {
              if (d["acct"][0]!=d["acct"][i])
                 break;
          }
          if (i==d["acct"].length)
              currColorS  = colorScales[d["acct"][0]]
          else
              currColorS  = colorScales["etc"]
       }
    }
    return currColorS
};

function createGPUHeatMap(svg, gpuData, acctColor, grpCnt, cntLine, gridSize) 
{
          var cards = svg.selectAll(".gpu")
              .data(gpuData);
          cards.enter().append("rect")
              .attr("x", function(d, i) { var x = d["gIdx"]; return x * gridSize; })
              .attr("y", function(d, i) { return d['gpuIdx'] * gridSize; })
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", function(d) { 
                                var str="bordered"; 
                                d["jobs"].forEach(function(e) {str += " " + e}); 
                                return str;})
              .attr("width",  gridSize)
              .attr("height", gridSize)
              .attr("title", function (d) {return d["name"]})
              .attr("value", function (d) {return d["gpu"+d["gpuIdx"]]})
              .on("dblclick", function(d) {
                              if (d3,event.ctrlKey) {
                                 location.href="jobDetails?jid="+d["jobs"]
                              } else {
                                 location.href="nodeDetails?node="+d["name"]
                              }})
              .on("click", function(d) {
                              var keyArr=d["jobs"];
                              keyArr.forEach(function(e) {
                                               $(".bordered").removeClass("popout");
                                               $(`.${e}`).addClass("popout")})})
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
          .attr("width",  width  + margin.left + margin.right)
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

