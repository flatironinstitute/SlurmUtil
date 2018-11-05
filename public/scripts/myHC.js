function graphSeries(dataSeries, chartTag, title, xCap, xLabel, yLabel) {
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
                     crosshair: true,
                     max:       xCap
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
                         pointWidth: 15
                     }
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
	 Highcharts.setOptions({
             global: {
		 useUTC: false
             }
});
