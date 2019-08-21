let bar = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "type",
      "type": "nominal",
      "title": "activity type",
    },
    "y": {
      "field": "count",
      "type": "quantitative",
      "title": "count",
    },
    // "color": {
    //   "field": "type",
    //   "type": "nominal",
    //   "title": "activity type",
    // }
  }
};

async function main() {
  let chart = await vegaEmbed(
    '#charts',
    bar,
    { actions: false }
  );

  return chart.view;
}

const socket = new WebSocket('ws:127.0.0.1:3012');

async function main() {
  let khop = await vegaEmbed(
    '#khop',
    bar,
    { actions: false }
  );

  let khopWeighted = await vegaEmbed(
    '#khop-weighted',
    bar,
    { actions: false }
  );

  socket.onmessage = e => {
    const [[activity, [count, weighted]], t, diff] = JSON.parse(e.data);
    const khopChanges = [{ type: activity, count: count }];
    const khopWeightedChanges = [{ type: activity, count: weighted }];

    if (diff > 0) {
      document.getElementById("k-epoch").innerHTML = "Epoch: " + t;
      document.getElementById("k-weighted-epoch").innerHTML = "Epoch: " + t;

      khop.view
        .change('table', vega.changeset().insert(khopChanges))
        .run();
      khopWeighted.view
        .change('table', vega.changeset().insert(khopWeightedChanges))
        .run();
    } else {
      khop.view
        .change('table', vega.changeset().remove(khopChanges))
        .run();
      khopWeighted.view
        .change('table', vega.changeset().insert(khopWeightedChanges))
        .run();
    }
  };
}

main();
