var makeCallback = function(chart, measure) {
    return function(data) {
        console.log(data);

        data.elements.forEach(function(entry) {
            chart.data.labels.push(entry.date);

            chart.data.datasets.forEach(function(dataset) {
                dataset.data.push(entry[measure]);
            });
        });

        chart.update();
    }
};
