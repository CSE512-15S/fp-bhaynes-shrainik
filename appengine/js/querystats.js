var updateQueryStats = function(element, queryStatus) {
    var instantiateStats = function(element, shuffledTuples, transferredTuples, elapsedNanos) {
        $(element.node()).empty();
        var div = element.append("div")
            .attr("class", "query-stats");
        var h = div.append("h4")
            .text("Query stats:");

        var items = "";
        items += templates.defItem({key: "Running time:", value: customFullTimeFormat(elapsedNanos, false)});
        items += templates.defItem({key: "# transferred elements:", value: Intl.NumberFormat().format(transferredTuples)});
        items += templates.defItem({key: "# shuffled tuples:", value: Intl.NumberFormat().format(shuffledTuples)});
        var dl = templates.defList({items: items});
        $(".query-stats").append(dl);
    }

    var getTuples = function(queryId, data, callback) {
        var shuffleUrl = templates.urls.aggregatedSentData({
                myria: myriaConnection,
                query: queryId,
                subquery: data[0],
                fragmentId: data[1],
                system: data[2]
            });

        d3.csv(shuffleUrl, function(d) {d.numTuples = +d.numTuples; return d; },
                           callback);
    }

    var getTransferredElements = function(queryId, data, aggregate, callback) {
        if(data.length == 0)
            callback(aggregate);
        else if(!data[0][0])
            return getTransferredElements(queryId, data.splice(0, 1), aggregate, callback);
        else
            return getTuples(queryId, data.shift(), function(d) {
                var tuples = d.reduce(function(a,b) { return a + b.numTuples; }, 0);
                return getTransferredElements(queryId, data, aggregate + tuples, callback);
            })
    }

    getTuples(queryStatus.queryId, [queryStatus.subqueryId, -1, 'Myria'], function(data) {
        var shuffledTuples = data.reduce(function(a,b) { return a + b.numTuples; }, 0);
        var transferQueryData = _.map(queryStatus.plan.fragments, function(f) { return [f.queryId, f.fragmentIndex, f.system] });

        getTransferredElements(queryStatus.queryId, transferQueryData, 0, function(transferredTuples) {
            instantiateStats(element, shuffledTuples, transferredTuples, queryStatus.elapsedNanos);
        });
    });
};
