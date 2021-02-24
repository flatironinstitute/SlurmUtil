//add a hide button to chart to hide all series of the chart
function addHideButton(chart) {
   chart.hideButton = chart.renderer.button('Hide All Series',chart.plotWidth-50, chart.plotTop-10)
                                    .attr({zIndex:3})
                                    .on('click', function () {
                                        chart.series.forEach(function (item, index) {
                                            item.hide();
                                        });
                                     })
                                    .add();
}

// two y Axis, stackcolumn and cdf
function graphSeries_stackColumn_CDF(dataSeries, chartTag, title, xType, xMax, xLabel, yLabel, pf_func=undefined) {
    //console.log(dataSeries)
    $('#'+chartTag).highcharts({
		 chart: {
		     panKey:   'shift',
		     panning:  true,
		     type:     'column',
		     zoomType: 'xy'
		 },
		 title: {
		     text: title,
		 },
		 credits: {
		     enabled: false,
		 },
		 subtitle: {
		     text: document.ontouchstart === undefined ?
                     'Click and drag in the plot area to zoom in. Shift-click to pan.' : 'Pinch the chart to zoom in'
		 },
		 xAxis: {
                     title: { text: xLabel},
                     type:  xType,
                     crosshair: true,
                     max:       xMax
		 },
		 yAxis: [{
		    title: {
		      text: yLabel } },{
                    title: {
                      text: "" },
                    max: 100,
                    min: 0,
                    opposite: true,
                    labels: {
                       format: "{value}%"
                    }
                 }],
		 legend: {
		    enabled: true
		 },
                 plotOptions: {
                    column: {
                         pointWidth: 10, 
                         tooltip: {
                             pointFormatter: pf_func
                         },
                         stacking:   'normal'
                    }
                 },
		 series: dataSeries
    });
}
Highcharts.setOptions({
             global: {
		 useUTC: false
             }
});
function graphSeries(dataSeries, chartTag, title, xType, xMax, xLabel, yLabel, pf_func=pointFormat_func) {
    //console.log('graphSeries data=', dataSeries)

	     $('#'+chartTag).highcharts({
		 chart: {
		     panKey:   'shift',
		     panning:  true,
		     type:     'column',
		     zoomType: 'xy'
		 },
		 title: {
		     text: title,
		 },
		 credits: {
		     enabled: false,
		 },
		 subtitle: {
		     text: document.ontouchstart === undefined ?
                     'Click and drag in the plot area to zoom in. Shift-click to pan.' : 'Pinch the chart to zoom in'
		 },
		 xAxis: {
                     title: { text: xLabel},
                     type:  xType,
                     crosshair: true,
                     max:       xMax
		 },
		 yAxis: [{
		    title: {
		      text: yLabel } },{
                    title: {
                      text: "" },
                    max: 100,
                    min: 0,
                    opposite: true,
                    labels: {
                       format: "{value}%"
                    }
                 }],
		 legend: {
		    enabled: true
		 },
                 plotOptions: {
                    column: {
                       pointWidth: 10
                    },
                    tooltip: {
                       pointFormatter: pf_func
                    },

                 },
		 series: [{
                    type: 'pareto',
                    name: 'CDF',
                    yAxis: 1,
                    zIndex: 10,
                    baseSeries: 1
                 }, {
                    name: 'Job Count',
                    type: 'column',
                    zIndex: 2,
                    data: dataSeries
                 }]
	     });
}

//series with x-Axis as time
//aSeries is used to annote event
function timeSeriesWithAnnotation(series, chartTag, title, yCap, aSeries, aUnit='', chart_type='area', yAxis_max=null) {
    console.log(title, yAxis_max)
    console.log(series)
    if ( aSeries.length > 0 ) {
                cus_labels = aSeries.map(function(x) {return crtLabel(x[1], x[2], x[0])})
                console.log(cus_labels)
                cus_anno = [{
                   labelOptions: {
                        backgroundColor: 'rgba(255,255,255,0.5)',
                    },
                    labels: cus_labels
                 }]
    } else
                cus_anno = []

    $('#'+chartTag).highcharts({
                 chart: {
                     panKey: 'shift',
                     panning: true,
                     type:    chart_type,
                     zoomType: 'x'
                 },
                 title: {
                     text: title,
                 },
                 credits: {
                     enabled: false,
                 },
                 subtitle: {
                     text: document.ontouchstart === undefined ?
                     'Click and drag in the plot area to zoom in. Shift-click to pan.' : 'Pinch the chart to zoom in'
                 },
                 annotations: cus_anno,
                 xAxis: {
                     type: 'datetime'
                 },
                 yAxis: {
                     title: {
                         text: yCap
                     },
                     max: yAxis_max
                 },
                 legend: {
                     enabled: true
                 },
                 plotOptions: {
                     area: {
                         stacking: 'normal',
                         lineColor: '#666666',
                         lineWidth: 1,
                         marker: {
                             lineWidth: 1,
                             lineColor: '#666666'
                         }
                     }
                 },
                 series: series
    });
}

function pf_logBucket() {
   //for x, let l=log(x) it counts 10^(l-0.05) - 10^(l+0.05
   lx    = Math.log10(this.x)
   lower = Math.pow(10, lx-0.05)
   upper = Math.pow(10, lx+0.05)
   return '<br/>' + this.series.name +':<b>[' + Math.ceil(lower) + '-' + Math.floor(upper) + '] '+ getDisplayN(this.y) + '</b>';
}
function pointFormat_func() {
   return '<br/>' + Highcharts.dateFormat('%b %e %H:%M:%S', new Date(this.x)) + ', ' + getDisplayN(this.y);
}
function pointFormat_func_KB() {
   return '<br/>' + Highcharts.dateFormat('%b %e %H:%M:%S', new Date(this.x)) + ', ' + getDisplayK(this.y) + 'B';
}

function timeSeriesScatterPlot_KB(series, chartTag, title, yCap, aSeries, pf_func=pointFormat_func_KB) {
   timeSeriesScatterPlot(series, chartTag, title, yCap, aSeries, pf_func)
}
function timeSeriesScatterPlot(series, chartTag, title, yCap, aSeries, pf_func=pointFormat_func) {
   timeSeriesPlot(series, "scatter", chartTag, title, yCap, aSeries, pf_func)
}
function timeSeriesColumnPlot(series, chartTag, title, yCap, aSeries, pf_func=pointFormat_func) {
   timeSeriesPlot(series, "column", chartTag, title, yCap, aSeries, pf_func)
}
function crtLabel (vx, vy, txt, aUnit='') {
   return { point: { xAxis: 0,
                     yAxis: 0,
                     x    : vx,
                     y    : vy},
            text: txt.toString().concat(aUnit) }
}
function timeSeriesPlot(series, chartType, chartTag, title, yCap, aSeries, pf_func=pointFormat_func) {
   console.log('timeSeriesPlot chartType=", chartType, ",series=', series)
   if ( aSeries.length > 0 ) {
                baseY = series[0]['data'][0][1]
                cus_labels = aSeries.map(function(x) {return crtLabel(x[0], baseY, x[1])})
                cus_anno = [{
                   labelOptions: {
                        backgroundColor: 'rgba(255,255,255,0.5)',
                    },
                    labels: cus_labels
                 }]
   } else
                cus_anno = []

   if (pf_func == pointFormat_func_KB) {
      Highcharts.setOptions({
       lang: {
           numericSymbols: ['k KB', 'M KB', 'G KB', 'T KB', 'P KB', 'E KB']
       }
      });
   } else {
      Highcharts.setOptions({
       lang: {
           numericSymbols: ["k", "M", "G", "T", "P", "E"]
       }
      });

   }
   var chart=$('#'+chartTag).highcharts({
                 chart: {
                     panKey:   'shift',
                     panning:  true,
                     type:     chartType,
                     zoomType: 'xy'
                 },
                 title: {
                     text: title,
                 },
                 credits: {
                     enabled: false,
                 },
                 subtitle: {
                     text: document.ontouchstart === undefined ?
                     'Click and drag in the plot area to zoom in. Shift-click to pan.' : 'Pinch the chart to zoom in'
                 },
                 annotations: cus_anno,
                 xAxis: {
                     type: 'datetime'
                 },
                 yAxis: {
                     title: {
                         text: yCap 
                     },
                 },
                 legend: {
                     enabled: true
                 },
                 plotOptions: {
                     scatter: {
                         marker: { radius: 2, },
                         tooltip: {
                             pointFormatter: pf_func
                         },
                         jitter: {
                             x: 0.5,
                             y: 0
                         },
                     },
                     line: {
                         tooltip: {
                             pointFormatter: pf_func
                         },
                     },
                 },
                 series: series
           }, addHideButton); 
}
function bubblePlot (series, container_id, title) {
   console.log('bubblePlot", ",series=', series)

   var chart = Highcharts.chart(container_id, {
   //var chart = $(container_sel).highcharts({
       bubble: {
           minSize: 3,
           maxSize: 10,
       },
       chart: {
           type:     'bubble',
           panKey:   'shift',
           panning:  true,
           zoomType: 'xy',
           plotBorderWidth: 1
       },
       title: {
           text: title,
       },
       credits: {
           enabled: false,
       },
       subtitle: {
           text: document.ontouchstart === undefined ?
           'Click and drag in the plot area to zoom in. Shift-click to pan.' : 'Pinch the chart to zoom in'
       },
       xAxis: {
           type: 'logarithmic',
           title: {
              text: 'Total bytes'
           },
           gridLineWidth: 1,
       },
       yAxis: {
           type: 'logarithmic',
           title: {
              text: 'File count'
           },
           startOnTick: false,
           endOnTick:   false,
           maxPadding:  0.2,
       },
       legend: {
           enabled: false,
       },
       tooltip: {
           useHTML:        true,
           headerFormat:   '<table>',
           pointFormatter: function(){ return '<tr><th colspan="2"><h3>' + this.name + '</h3></th></tr>' +
                                        '<tr><th>Bytes:</th><td>' + this.x.toExponential(3) + '</td></tr>' +
                                        '<tr><th>Files:</th><td>' + this.y.toExponential(3) + '</td></tr>' +
                                        '<tr><th>Delta bytes:</th><td>' + this.dfb.toExponential(3) + '</td></tr>' +
                                        '<tr><th>Delta files:</th><td>' + this.dfc + '</td></tr>' } ,
           footerFormat:   '</table>',
           followPointer:  true
       },
       plotOptions: {
           series: {
             dataLabels: {
                enabled: true,
                format: '{point.name}'
             },
             events: {
                click: function(event) { // double click to see the history
                   location.replace("./user_fileReport?user="+event.point.name)
                },
             }
           },
       },
       series: [{
           data: series
       }]
   });
   console.log("chart=", chart)
   return chart
}
