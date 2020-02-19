function graphSeries_stackColumn_CDF(dataSeries, chartTag, title, xType, xMax, xLabel, yLabel) {
             console.log(dataSeries)
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
function graphSeries(dataSeries, chartTag, title, xType, xMax, xLabel, yLabel) {
    console.log('graphSeries data=', dataSeries)

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
function timeSeriesWithAnnotation(series, chartTag, title, yCap, aSeries, aUnit='', chart_type='area') {
    function crtLabel (vx, vy, v) {
                return {
                      point: {
                          xAxis: 0,
                          yAxis: 0,
                          x    : vx,
                          y    : vy
                      },
                      text: v.toString().concat(aUnit) }
    }

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
                     }
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

function pointFormat_func() {
   return '<br/>' + Highcharts.dateFormat('%b %e %H:%M:%S', new Date(this.x)) + ', ' + this.y.toFixed(2);
}
function pointFormat_func_KB() {
   return '<br/>' + Highcharts.dateFormat('%b %e %H:%M:%S', new Date(this.x)) + ', ' + this.y + ' KB';
}

function timeSeriesScatterPlot_KB(series, chartTag, title, yCap, aSeries, pf_func=pointFormat_func_KB) {
   timeSeriesScatterPlot(series, chartTag, title, yCap, aSeries, pf_func)
}
function timeSeriesScatterPlot(series, chartTag, title, yCap, aSeries, pf_func=pointFormat_func) {
             function crtLabel (vx, vy, txt) {
                return {
                      point: {
                          xAxis: 0,
                          yAxis: 0,
                          x    : vx,
                          y    : vy
                      },
                      text: txt }
             }

             console.log(series)
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

             $('#'+chartTag).highcharts({
                 chart: {
                     panKey: 'shift',
                     panning: true,
                     type: 'scatter',
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
                     }
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
                 },
                 series: series
             });
}
