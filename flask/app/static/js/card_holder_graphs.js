$(document).ready(function() {


        $(chart_id1).highcharts({
                chart: chart1,
                title: title1,
                xAxis: {
                    type: 'category',
                    labels: {
                        rotation: -45,
                        style: {
                            fontSize: '13px',
                            fontFamily: 'Verdana, sans-serif'
                        }
                    }
                },
                yAxis: {

                    title: {
                        min:0,
                        text: 'Total Daily Transactions ($)'
                    }
                },
                legend: {
                    enabled: false
                },
                tooltip: {
                    pointFormat: 'Transactions: <b>${point.y:.2f}</b>'
                },
                series: [{
                    name: 'Transactions',
                    data:series1,
               }]
        });

        $(chart_id2).highcharts({
                chart: chart2,
                title: title2,
                xAxis: {
                    type: 'category',
                    labels: {
                        rotation: -45,
                        style: {
                            fontSize: '13px',
                            fontFamily: 'Verdana, sans-serif'
                        }
                    }
                },
                yAxis: {

                    title: {
                        min:0,
                        text: 'Total Monthly Transactions ($)'
                    }
                },
                legend: {
                    enabled: false
                },
                tooltip: {
                    pointFormat: 'Transactions: <b>${point.y:.2f}</b>'
                },
                series: [{
                    name: 'Transactions',
                    data:series2,
                }]
        });
        
	$(chart_id3).highcharts({
                chart: chart3,
                title: title3,
                xAxis: {
                    type: 'category',
                    labels: {
                        rotation: -45,
                        style: {
                            fontSize: '13px',
                            fontFamily: 'Verdana, sans-serif'
                        }
                    }
                },
                yAxis: {

                    title: {
                        min:0,
                        text: 'Total Daily Transactions ($)'
                    }
                },
                legend: {
                    enabled: false
                },
                tooltip: {
                    pointFormat: 'Transactions: <b>${point.y:.2f}</b>'
                },
                series: [{
                    name: 'Transactions',
                    data:series3,
               }]
        });

 

/*



	$(chart_id1).highcharts({
		chart: chart1,
		title: title1,
		xAxis: xAxis1,
		yAxis: yAxis1,
		series: series1
	});

	$(chart_id2).highcharts({
		chart: chart2,
		title: title2,
		xAxis: xAxis2,
		yAxis: yAxis2,
		series: series2
	});
	$(chart_id3).highcharts({
		chart: chart3,
		title: title3,
		xAxis: xAxis3,
		yAxis: yAxis3,
		series: series3
	});
*/

	$(chart_id4).highcharts({
		chart: chart4,
		title: title4,
		xAxis: xAxis4,
		yAxis: yAxis4,
		series: series4,
  /*              tooltip: {
                    pointFormat: 'Transactions: <b>${point.y:.2f}</b>'
    */   	});


	$(chart_id5).highcharts({
		chart: chart5,
		title: title5,
		xAxis: xAxis5,
		yAxis: yAxis5,
		series: series5,
                tooltip: {
                    pointFormat: 'Transactions: <b>${point.y:.2f}</b>'
                }
	});




});
