let bar = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "type",
      "type": "nominal",
      "title": "activity type"
    },
    "y": {
      "field": "count",
      "type": "quantitative",
      "title": "count"
    }
  }
};

function Charts() {
  var _React$useState = React.useState(0),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      count = _React$useState2[0],
      setCount = _React$useState2[1];

  var _React$useState3 = React.useState([]),
      _React$useState4 = _slicedToArray(_React$useState3, 2),
      vis = _React$useState4[0],
      setVis = _React$useState4[1];

  var _React$useState5 = React.useState(0),
      _React$useState6 = _slicedToArray(_React$useState5, 2),
      epoch = _React$useState6[0],
      setEpoch = _React$useState6[1];

  var refs = [React.useRef(null), React.useRef(null)];

  React.useEffect(function () {
    var promises = refs.map(function (ref) {
      return vegaEmbed(ref.current, bar, { actions: false });
    });
    Promise.all(promises).then(function (ress) {
      return setVis(ress.map(function (res) {
        return res.view;
      }));
    });
  }, []);

  React.useEffect(function () {
    if (vis.length > 0) {
      socket.onmessage = function (e) {
        var data = JSON.parse(e.data);
        var agg = data.filter(function (datum) {
          return datum["Agg"] !== undefined;
        }).map(function (datum) {
          return Object.values(datum)[0];
        });
        var agg_count = agg.map(function (_ref) {
          var _ref2 = _slicedToArray(_ref, 2),
              _ref2$ = _slicedToArray(_ref2[0], 2),
              type = _ref2$[0],
              _ref2$$ = _slicedToArray(_ref2$[1], 2),
              count = _ref2$$[0],
              weighted = _ref2$$[1],
              t = _ref2[1];

          return { type: type, count: count };
        });
        var agg_weighted = agg.map(function (_ref3) {
          var _ref4 = _slicedToArray(_ref3, 2),
              _ref4$ = _slicedToArray(_ref4[0], 2),
              type = _ref4$[0],
              _ref4$$ = _slicedToArray(_ref4$[1], 2),
              count = _ref4$$[0],
              weighted = _ref4$$[1],
              t = _ref4[1];

          return { type: type, count: weighted };
        });

        var toPlot = [agg_count, agg_weighted];

        if (agg.length > 0) {
          setEpoch(agg[agg.length - 1][1]);
        }

        var all = data.filter(function (datum) {
          return datum["All"] !== undefined;
        }).map(function (datum) {
          return Object.values(datum)[0];
        });
        all.forEach(function (d) {
          return toHighlight.add("" + d[0] + d[1]);
        });
        if (all.length > 0) {
          update();
        }

        vis.forEach(function (v, idx) {
          return v.change('table', vega.changeset().insert(toPlot[idx])).run();
        });
      };
    }
  });

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      null,
      "5 hops (epoch ",
      epoch,
      ")"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      refs.map(function (ref, idx) {
        return React.createElement("div", { key: idx, ref: ref });
      })
    )
  );
}

var domContainer = document.querySelector('#react-container');
ReactDOM.render(React.createElement(Charts, null), domContainer);