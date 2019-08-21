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
    let data = JSON.parse(e.data);
    const agg = data
      .filter(datum => datum["Agg"] !== undefined)
      .map(datum => Object.values(datum)[0]);
    const agg_count = agg.map(([[type, [count, weighted]], t]) => ({ type, count }));
    const agg_weighted = agg.map(([[type, [count, weighted]], t]) => ({ type, count }));

    khop.view.change('table', vega.changeset().insert(agg_count)).run();
    khopWeighted.view.change('table', vega.changeset().insert(agg_weighted)).run();
    if (agg.length > 0) {
      document.getElementById("k-epoch").innerHTML = "Epoch: " + agg[agg.length - 1][1];
      document.getElementById("k-weighted-epoch").innerHTML = "Epoch: " + agg[agg.length - 1][1];
    }

    const all = data
      .filter(datum => datum["All"] !== undefined)
      .map(datum => Object.values(datum)[0]);
    all.forEach(d => toHighlight.add(`${d[0]}${d[1]}`));
    console.log(toHighlight);
    update();
  };
}

main();
