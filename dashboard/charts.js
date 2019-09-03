var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _types;

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

var khopsChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "ca",
      "type": "nominal",
      "title": "activity type"
    },
    "y": {
      "field": "ac",
      "type": "quantitative",
      "title": "count"
    }
  }
};

var weightedKhopsChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "ca",
      "type": "nominal",
      "title": "activity type"
    },
    "y": {
      "field": "wac",
      "type": "quantitative",
      "title": "count"
    }
  }
};

var activityCountChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "wf",
      "type": "nominal",
      "title": "worker id"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "# activities",
      "field": "ac"
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

var activityDurationChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "wf",
      "type": "nominal",
      "title": "worker id"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "t activities (ns)",
      "field": "at"
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

var msgsCountChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "cw",
      "type": "nominal",
      "title": "worker ids"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "# activities",
      "field": "ac"
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

var msgsDurationChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "cw",
      "type": "nominal",
      "title": "worker ids"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "t activities (ns)",
      "field": "at"
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

var recordSentChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "cw",
      "type": "nominal",
      "title": "worker ids"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "# records",
      "field": "rc"
    }
  }
};

var recordProcessedChart = {
  "width": 300,
  "autosize": { "resize": true },
  "mark": "bar",
  "data": { "name": "table" },
  "encoding": {
    "x": {
      "field": "wf",
      "type": "nominal",
      "title": "worker id"
    },
    "y": {
      "aggregate": "sum",
      "type": "quantitative",
      "title": "# records",
      "field": "rc"
    }
  }
};

var types = (_types = {}, _defineProperty(_types, "Processing", "#0b6623"), _defineProperty(_types, "Spinning", "#e48282"), _defineProperty(_types, "ControlMessage", "#4b5f53"), _defineProperty(_types, "DataMessage", "#971757"), _defineProperty(_types, "Waiting", "#FF0000"), _defineProperty(_types, "Busy", "#059dc0"), _types);

var margins = {
  left: 10,
  top: 10,
  right: 30
};

var pag = [];

var genTitle = function genTitle(d) {
  return d.type + " " + (d.o === 0 ? '' : 'Op' + d.o + ' ') + (d.l > 0 ? '(' + d.l + ')' : '');
};

var tooltip = d3.select("body").append("div").attr("class", "tooltip").style("opacity", 0);

var makeTooltip = function makeTooltip(d) {
  return "\n    w" + d.src.w + ", " + d.src.t / 1000000 + " -> w" + d.dst.w + ", " + d.dst.t / 1000000 + " <br>\n    type: " + d.type + " <br>\n    operator: " + d.o + " <br>\n    length: " + d.l + " <br>\n    traversal: " + d.tr + " <br>";
};

var pagState = {};
var toHighlight = new Set();

function setPAGEpoch() {
  if (pag.length < 2) {
    return;
  }

  pagState.x = d3.scaleLinear().domain([pag[0].src.t / 1000000, pag[pag.length - 1].dst.t / 1000000]);
  pagState.xAxis = d3.axisBottom(pagState.x);

  pagState.activities.selectAll("line").data(pag).enter().append('line').attr('class', 'activity').on("mouseover", function (d) {
    tooltip.transition().style("opacity", 1);
    tooltip.html(makeTooltip(d)).style("left", d3.event.pageX + "px").style("top", d3.event.pageY + 20 + "px");
  }).on("mouseout", function (d) {
    return tooltip.transition().style("opacity", 0);
  });

  pagState.activities.selectAll("line").data(pag).exit().remove();

  pagState.activities.selectAll("text").data(pag).enter().append('text').attr('class', 'label');

  pagState.activities.selectAll("text").data(pag).exit().remove();

  pagState.svgParent.transition().duration(50).call(pagState.zoom.transform, d3.zoomIdentity);
}

function updatePAG() {
  if (pag.length < 2) {
    return;
  }

  var highlight = document.getElementById("hop-highlight").checked;
  var width = window.innerWidth;

  var domain = [pag[0].src.t / 1000000, pag[pag.length - 1].dst.t / 1000000];
  pagState.x = d3.scaleLinear().domain(domain);
  pagState.x.range([margins.left, width - margins.right]);

  pagState.svgParent.call(pagState.zoom);
  var xt = pagState.lastTransform.rescaleX(pagState.x);

  pagState.svgXAxis.attr("transform", "translate(0," + margins.top + ")").call(pagState.xAxis.scale(xt));

  pagState.activities.selectAll("line").data(pag).attr('x1', function (d) {
    return xt(d.src.t / 1000000);
  }).attr('y1', function (d) {
    return 100 * (1 + d.src.w);
  }).attr('stroke', function (d) {
    return types[d.type];
  }).attr('stroke-width', function (d) {
    return d.src.w !== d.dst.w ? 1.5 : 2;
  }).attr('x2', function (d) {
    return xt(d.dst.t / 1000000);
  }).attr('y2', function (d) {
    return 100 * (1 + d.dst.w);
  }).attr('stroke-opacity', function (d) {
    if (toHighlight.size > 0 && highlight) {
      return toHighlight.has("" + d.src.t + d.dst.t) ? 1 : 0.1;
    } else {
      return 1;
    }
  }).attr('stroke-dasharray', function (d) {
    return d.src.w !== d.dst.w ? "5,5" : "5,0";
  });

  pagState.activities.selectAll("text").data(pag).attr("x", function (d) {
    return xt((d.src.t + d.dst.t) / (2 * 1000000)) - 30;
  }).attr("y", function (d) {
    return (1 + d.src.w + (1 + d.dst.w)) / 2 * 100 - 10;
  }).attr("fill", function (d) {
    return types[d.type];
  }).attr("opacity", function (d) {
    if (toHighlight.size > 0 && highlight) {
      return toHighlight.has("" + d.src.t + d.dst.t) ? 1 : 0.1;
    } else {
      return 1;
    }
  }).text(genTitle);
}

var socket = new WebSocket('ws:127.0.0.1:3012');

function App() {
  var _React$useState = React.useState(1),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      epoch = _React$useState2[0],
      setEpoch = _React$useState2[1];

  var _React$useState3 = React.useState(true),
      _React$useState4 = _slicedToArray(_React$useState3, 2),
      highlight = _React$useState4[0],
      setHighlight = _React$useState4[1];

  var _React$useState5 = React.useState(true),
      _React$useState6 = _slicedToArray(_React$useState5, 2),
      showWaiting = _React$useState6[0],
      setShowWaiting = _React$useState6[1];

  var _React$useState7 = React.useState(false),
      _React$useState8 = _slicedToArray(_React$useState7, 2),
      splitWorker = _React$useState8[0],
      setSplitWorker = _React$useState8[1];

  React.useEffect(function () {
    var svgParent = d3.select("#d3").append("svg").attr("id", "graph");
    var svg = svgParent.append("g");
    var svgXAxis = svg.append("g").attr("class", "axis axis--x");
    var activities = svg.append("g");
    pagState = {
      epoch: epoch,
      lastTransform: d3.zoomIdentity,
      x: null,
      xAxis: null,
      zoom: d3.zoom().scaleExtent([1, 512]).on("zoom", function () {
        pagState.lastTransform = d3.event.transform;
        updatePAG();
      }),
      svgParent: svgParent,
      svg: svg,
      svgXAxis: svgXAxis,
      activities: activities
    };

    socket.addEventListener("message", function (e) {
      var _JSON$parse = JSON.parse(e.data),
          type = _JSON$parse.type,
          payload = _JSON$parse.payload;

      if (type === "ALL") {
        payload.forEach(function (d) {
          return toHighlight.add("" + d[0] + d[1]);
        });
        updatePAG();
      } else if (type == "PAG") {
        pag = payload;
        setPAGEpoch(pagState);
      }
    });

    socket.send(JSON.stringify({ type: 'PAG', epoch: epoch }));
    socket.send(JSON.stringify({ type: 'AGG', epoch: epoch }));
    socket.send(JSON.stringify({ type: 'ALL', epoch: epoch }));
    socket.send(JSON.stringify({ type: 'MET', epoch: epoch }));
    d3.select(window).on('resize', updatePAG());

    socket.send(JSON.stringify({ type: 'INV' }));
    setInterval(function () {
      socket.send(JSON.stringify({ type: 'INV' }));
    }, 5000);
  }, []);

  var epochUpdate = function epochUpdate(e) {
    var epoch = parseInt(e.target.value);
    setEpoch(epoch || '');
    if (epoch) {
      socket.send(JSON.stringify({ type: 'PAG', epoch: epoch }));
      socket.send(JSON.stringify({ type: 'AGG', epoch: epoch }));
      socket.send(JSON.stringify({ type: 'ALL', epoch: epoch }));
      socket.send(JSON.stringify({ type: 'MET', epoch: epoch }));
      pagState = Object.assign({}, pagState, { epoch: epoch });
    }
  };

  var highlightUpdate = function highlightUpdate(e) {
    updatePAG();
    setHighlight(e.target.checked);
  };

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      { style: { textAlign: "center" } },
      " ST2 Dashboard"
    ),
    React.createElement(
      "h1",
      null,
      "PAG Viz"
    ),
    React.createElement("div", { id: "d3" }),
    React.createElement(
      "div",
      { className: "viz-settings" },
      React.createElement(
        "b",
        { style: { marginRight: "6px" } },
        "Highlight hops: "
      ),
      React.createElement("input", { id: "hop-highlight", type: "checkbox", style: { marginRight: "24px" }, checked: highlight, onChange: highlightUpdate }),
      React.createElement(
        "b",
        { style: { marginRight: "6px" } },
        "Epoch: "
      ),
      React.createElement("input", { id: "epoch", type: "text", value: epoch, onChange: epochUpdate })
    ),
    React.createElement(
      "div",
      { style: { flex: "0 1 auto" } },
      React.createElement(
        "b",
        { style: { marginRight: "6px" } },
        "Show waiting/busy: "
      ),
      React.createElement("input", { type: "checkbox", style: { marginRight: "24px" }, checked: showWaiting, onChange: function onChange(e) {
          return setShowWaiting(e.target.checked);
        } }),
      React.createElement(
        "b",
        { style: { marginRight: "6px" } },
        "Split by worker: "
      ),
      React.createElement("input", { type: "checkbox", checked: splitWorker, onChange: function onChange(e) {
          return setSplitWorker(e.target.checked);
        } })
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      React.createElement(KHops, { epoch: epoch, showWaiting: showWaiting, splitWorker: splitWorker }),
      React.createElement(ActivityMetrics, { epoch: epoch, showWaiting: showWaiting, splitWorker: splitWorker }),
      React.createElement(CrossMetrics, { epoch: epoch, showWaiting: showWaiting, splitWorker: splitWorker }),
      React.createElement(RecordMetrics, { epoch: epoch, showWaiting: showWaiting, splitWorker: splitWorker })
    ),
    React.createElement(Invariants, null)
  );
}

var prepper = function prepper(data, sumKey, showWaiting, splitWorker) {
  var filtered = data.filter(function (_ref) {
    var a = _ref.a;
    return showWaiting || !a.startsWith("Wait") && !a.startsWith("Bus");
  });

  if (splitWorker) {
    return filtered;
  } else {
    return filtered.reduce(function (acc, d) {
      var idx = acc.findIndex(function (x) {
        return x.a === d.a;
      });
      if (idx > -1) {
        acc[idx][sumKey] += d[sumKey];
        return acc;
      } else {
        var _ref2;

        return [].concat(_toConsumableArray(acc), [(_ref2 = { a: d.a }, _defineProperty(_ref2, sumKey, d[sumKey]), _defineProperty(_ref2, "wf", "all"), _defineProperty(_ref2, "cw", "all"), _ref2)]);
      }
    }, []);
  }
};

function RecordMetrics(_ref3) {
  var epoch = _ref3.epoch,
      showWaiting = _ref3.showWaiting,
      splitWorker = _ref3.splitWorker;

  // Plot 1: # records sent by each worker to each worker
  var _React$useState9 = React.useState(undefined),
      _React$useState10 = _slicedToArray(_React$useState9, 2),
      p1 = _React$useState10[0],
      setP1 = _React$useState10[1];
  // Plot 2: # records processed by each worker


  var _React$useState11 = React.useState(undefined),
      _React$useState12 = _slicedToArray(_React$useState11, 2),
      p2 = _React$useState12[0],
      setP2 = _React$useState12[1];

  var _React$useState13 = React.useState([]),
      _React$useState14 = _slicedToArray(_React$useState13, 2),
      metricsData = _React$useState14[0],
      setMetricsData = _React$useState14[1];

  var p1Ref = React.useRef(null);
  var p2Ref = React.useRef(null);

  React.useEffect(function () {
    vegaEmbed(p1Ref.current, recordSentChart, { actions: false }).then(function (res) {
      return setP1(res.view);
    });
    vegaEmbed(p2Ref.current, recordProcessedChart, { actions: false }).then(function (res) {
      return setP2(res.view);
    });

    socket.addEventListener("message", function (e) {
      var _JSON$parse2 = JSON.parse(e.data),
          type = _JSON$parse2.type,
          payload = _JSON$parse2.payload;

      if (type === "MET") {
        setMetricsData(payload);
      }
    });
  }, []);

  React.useEffect(function () {
    if (p1) {
      var dataMsgs = metricsData.filter(function (d) {
        return d.a.startsWith("Dat");
      }).map(function (d) {
        return Object.assign({}, d, { cw: [d.wf, d.wt] });
      });
      p1.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(dataMsgs, "rc", showWaiting, splitWorker))).run();
    }

    if (p2) {
      var procMsgs = metricsData.filter(function (d) {
        return d.a.startsWith("Proc");
      });
      p2.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(procMsgs, "rc", showWaiting, splitWorker))).run();
    }
  });

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      { style: { marginRight: "18px" } },
      "Record Metrics (for epoch ",
      epoch,
      ")"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Records Sent"
        ),
        React.createElement("div", { ref: p1Ref })
      ),
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Records Processed"
        ),
        React.createElement("div", { ref: p2Ref })
      )
    )
  );
}

function CrossMetrics(_ref4) {
  var epoch = _ref4.epoch,
      showWaiting = _ref4.showWaiting,
      splitWorker = _ref4.splitWorker;

  // Plot 1
  // # data messages between workers
  var _React$useState15 = React.useState(undefined),
      _React$useState16 = _slicedToArray(_React$useState15, 2),
      p1 = _React$useState16[0],
      setP1 = _React$useState16[1];
  // Plot 2
  // t data messages between workers
  // t control messages between workers


  var _React$useState17 = React.useState(undefined),
      _React$useState18 = _slicedToArray(_React$useState17, 2),
      p2 = _React$useState18[0],
      setP2 = _React$useState18[1];

  var _React$useState19 = React.useState([]),
      _React$useState20 = _slicedToArray(_React$useState19, 2),
      metricsData = _React$useState20[0],
      setMetricsData = _React$useState20[1];

  var p1Ref = React.useRef(null);
  var p2Ref = React.useRef(null);

  React.useEffect(function () {
    vegaEmbed(p1Ref.current, msgsCountChart, { actions: false }).then(function (res) {
      return setP1(res.view);
    });
    vegaEmbed(p2Ref.current, msgsDurationChart, { actions: false }).then(function (res) {
      return setP2(res.view);
    });

    socket.addEventListener("message", function (e) {
      var _JSON$parse3 = JSON.parse(e.data),
          type = _JSON$parse3.type,
          payload = _JSON$parse3.payload;

      if (type === "MET") {
        setMetricsData(payload.filter(function (d) {
          return d.a.startsWith("Dat") || d.a.startsWith("Con");
        }).map(function (d) {
          return Object.assign({}, d, { cw: [d.wf, d.wt] });
        }));
      }
    });
  }, []);

  React.useEffect(function () {
    if (p1) {
      p1.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(metricsData, "ac", showWaiting, splitWorker))).run();
    }

    if (p2) {
      p2.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(metricsData, "at", showWaiting, splitWorker))).run();
    }
  });

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      { style: { marginRight: "18px" } },
      "Cross Metrics (for epoch ",
      epoch,
      ")"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Messages Count"
        ),
        React.createElement("div", { ref: p1Ref })
      ),
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Messages Duration"
        ),
        React.createElement("div", { ref: p2Ref })
      )
    )
  );
}

function ActivityMetrics(_ref5) {
  var epoch = _ref5.epoch,
      showWaiting = _ref5.showWaiting,
      splitWorker = _ref5.splitWorker;

  var _React$useState21 = React.useState(undefined),
      _React$useState22 = _slicedToArray(_React$useState21, 2),
      aC = _React$useState22[0],
      setAC = _React$useState22[1];

  var _React$useState23 = React.useState(undefined),
      _React$useState24 = _slicedToArray(_React$useState23, 2),
      aD = _React$useState24[0],
      setAD = _React$useState24[1];

  var _React$useState25 = React.useState([]),
      _React$useState26 = _slicedToArray(_React$useState25, 2),
      metricsData = _React$useState26[0],
      setMetricsData = _React$useState26[1];

  var aCRef = React.useRef(null);
  var aDRef = React.useRef(null);

  React.useEffect(function () {
    vegaEmbed(aCRef.current, activityCountChart, { actions: false }).then(function (res) {
      return setAC(res.view);
    });
    vegaEmbed(aDRef.current, activityDurationChart, { actions: false }).then(function (res) {
      return setAD(res.view);
    });

    socket.addEventListener("message", function (e) {
      var _JSON$parse4 = JSON.parse(e.data),
          type = _JSON$parse4.type,
          payload = _JSON$parse4.payload;

      if (type === "MET") {
        setMetricsData(payload);
      }
    });
  }, []);

  React.useEffect(function () {
    if (aC) {
      aC.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(metricsData, "ac", showWaiting, splitWorker))).run();
    }

    if (aD) {
      aD.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(prepper(metricsData, "at", showWaiting, splitWorker))).run();
    }
  });

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      { style: { marginRight: "18px" } },
      "Activity Metrics (for epoch ",
      epoch,
      ")"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Activity Count"
        ),
        React.createElement("div", { ref: aCRef })
      ),
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Activity Duration"
        ),
        React.createElement("div", { ref: aDRef })
      )
    )
  );
}

function KHops(_ref6) {
  var epoch = _ref6.epoch,
      showWaiting = _ref6.showWaiting,
      splitWorker = _ref6.splitWorker;

  var _React$useState27 = React.useState(undefined),
      _React$useState28 = _slicedToArray(_React$useState27, 2),
      vis = _React$useState28[0],
      setVis = _React$useState28[1];

  var _React$useState29 = React.useState(undefined),
      _React$useState30 = _slicedToArray(_React$useState29, 2),
      wVis = _React$useState30[0],
      setWVis = _React$useState30[1];

  var _React$useState31 = React.useState([]),
      _React$useState32 = _slicedToArray(_React$useState31, 2),
      visData = _React$useState32[0],
      setVisData = _React$useState32[1];

  var visRef = React.useRef(null);
  var wVisRef = React.useRef(null);

  var khopsPrepper = function khopsPrepper(data, sumKey, showWaiting, splitWorker) {
    var filtered = data.filter(function (_ref7) {
      var a = _ref7.a;
      return showWaiting || !a.startsWith("Wait") && !a.startsWith("Bus");
    });

    if (splitWorker) {
      return filtered;
    } else {
      return filtered.reduce(function (acc, d) {
        var idx = acc.findIndex(function (x) {
          return x.ca === d.ca;
        });
        if (idx > -1) {
          acc[idx][sumKey] += d[sumKey];
          return acc;
        } else {
          return [].concat(_toConsumableArray(acc), [_defineProperty({ ca: d.a }, sumKey, d[sumKey])]);
        }
      }, []);
    }
  };

  React.useEffect(function () {
    vegaEmbed(visRef.current, khopsChart, { actions: false }).then(function (res) {
      return setVis(res.view);
    });
    vegaEmbed(wVisRef.current, weightedKhopsChart, { actions: false }).then(function (res) {
      return setWVis(res.view);
    });

    socket.addEventListener("message", function (e) {
      var _JSON$parse5 = JSON.parse(e.data),
          type = _JSON$parse5.type,
          payload = _JSON$parse5.payload;

      if (type === "AGG") {
        setVisData(payload.map(function (d) {
          return Object.assign({}, d, { ca: [d.a, d.wf] });
        }));
      }
    });
  }, []);

  React.useEffect(function () {
    if (vis) {
      vis.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(khopsPrepper(visData, "ac", showWaiting, splitWorker))).run();
    }

    if (wVis) {
      wVis.change('table', vega.changeset().remove(function () {
        return true;
      }).insert(khopsPrepper(visData, "wac", showWaiting, splitWorker))).run();
    }
  });

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      { style: { marginRight: "18px" } },
      "K-Hops (up to epoch ",
      epoch,
      ")"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap" } },
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Unweighted"
        ),
        React.createElement("div", { ref: visRef })
      ),
      React.createElement(
        "div",
        null,
        React.createElement(
          "h2",
          null,
          "Weighted"
        ),
        React.createElement("div", { ref: wVisRef })
      )
    )
  );
}

var formatE = function formatE(e) {
  if (e.length > 0) {
    return e.sort(function (a, b) {
      return ns(b.to.timestamp) - ns(b.from.timestamp) - (ns(a.to.timestamp) - ns(a.from.timestamp));
    }).map(function (_ref9) {
      var from = _ref9.from,
          to = _ref9.to;

      var duration = ((ns(to.timestamp) - ns(from.timestamp)) / 1000000).toFixed(2);
      return "Epoch " + from.epoch + " took " + duration + "ms.";
    }).join("\n");
  } else {
    return "No invariants violated.";
  }
};

var ns = function ns(ts) {
  return ts.nanos + ts.secs * 1000000000;
};

var formatO = function formatO(e) {
  if (e.length > 0) {
    return e.sort(function (a, b) {
      return ns(b.from.destination.timestamp) - ns(b.from.source.timestamp) - (ns(a.from.destination.timestamp) - ns(a.from.source.timestamp));
    }).map(function (_ref10) {
      var from = _ref10.from,
          to = _ref10.to;

      var duration = ((ns(from.destination.timestamp) - ns(from.source.timestamp)) / 1000000).toFixed(2);
      return "Epoch " + from.source.epoch + " | w" + from.source.worker_id + " @ " + ns(from.source.timestamp) + ": Operator " + from.operator_id + " (" + from.edge_type + ") took " + duration + "ms.";
    }).join("\n");
  } else {
    return "No invariants violated.";
  }
};

var formatM = function formatM(e) {
  if (e.length > 0) {
    return e.sort(function (a, b) {
      return ns(b.msg.destination.timestamp) - ns(b.msg.source.timestamp) - (ns(a.msg.destination.timestamp) - ns(a.msg.source.timestamp));
    }).map(function (_ref11) {
      var msg = _ref11.msg;

      var duration = ((ns(msg.destination.timestamp) - ns(msg.source.timestamp)) / 1000000).toFixed(2);
      return "Epoch " + msg.source.epoch + " | w" + msg.source.worker_id + " @ " + ns(msg.source.timestamp) + " -> w" + msg.destination.worker_id + " @ " + ns(msg.destination.timestamp) + ": " + msg.edge_type + " took " + duration + "ms.";
    }).join("\n");
  } else {
    return "No invariants violated.";
  }
};

function Invariants() {
  var _React$useState33 = React.useState([]),
      _React$useState34 = _slicedToArray(_React$useState33, 2),
      mEpoch = _React$useState34[0],
      setMEpoch = _React$useState34[1];

  var _React$useState35 = React.useState([]),
      _React$useState36 = _slicedToArray(_React$useState35, 2),
      mOp = _React$useState36[0],
      setMOp = _React$useState36[1];

  var _React$useState37 = React.useState([]),
      _React$useState38 = _slicedToArray(_React$useState37, 2),
      mMsg = _React$useState38[0],
      setMMsg = _React$useState38[1];

  var _React$useState39 = React.useState(null),
      _React$useState40 = _slicedToArray(_React$useState39, 2),
      mE = _React$useState40[0],
      setME = _React$useState40[1];

  var _React$useState41 = React.useState(null),
      _React$useState42 = _slicedToArray(_React$useState41, 2),
      mO = _React$useState42[0],
      setMO = _React$useState42[1];

  var _React$useState43 = React.useState(null),
      _React$useState44 = _slicedToArray(_React$useState43, 2),
      mM = _React$useState44[0],
      setMM = _React$useState44[1];

  React.useEffect(function () {
    socket.addEventListener("message", function (e) {
      var _JSON$parse6 = JSON.parse(e.data),
          type = _JSON$parse6.type,
          payload = _JSON$parse6.payload;

      if (type === "INV") {
        var _e = [];
        var o = [];
        var m = [];

        payload.forEach(function (p) {
          if (p["Epoch"]) {
            if (!mE) {
              setME("" + p["Epoch"].max / 1000000);
            }
            _e.push(p["Epoch"]);
          } else if (p["Operator"]) {
            if (!mO) {
              setMO("" + p["Operator"].max / 1000000);
            }
            o.push(p["Operator"]);
          } else if (p["Message"]) {
            if (!mM) {
              setMM("" + p["Message"].max / 1000000);
            }
            m.push(p["Message"]);
          }
        });

        _e.length && setMEpoch(function (prev) {
          return prev.concat(_e);
        });
        o.length && setMOp(function (prev) {
          return prev.concat(o);
        });
        m.length && setMMsg(function (prev) {
          return prev.concat(m);
        });
      }
    });
  }, []);

  return React.createElement(
    "div",
    null,
    React.createElement(
      "h1",
      null,
      "Invariant Violations (over all epochs)"
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap", width: "100%" } },
      React.createElement(
        "h2",
        { className: "inv" },
        "Epoch Duration ",
        mE && "(max: " + mE + "ms)"
      ),
      React.createElement(
        "h2",
        { className: "inv" },
        "Operator Duration ",
        mO && "(max: " + mO + "ms)"
      ),
      React.createElement(
        "h2",
        { className: "inv" },
        "Message Duration ",
        mM && "(max: " + mM + "ms)"
      )
    ),
    React.createElement(
      "div",
      { style: { display: "flex", flexFlow: "row wrap", width: "100%" } },
      React.createElement("textarea", { className: "log inv", rows: "15", disabled: true, value: formatE(mEpoch) }),
      React.createElement("textarea", { className: "log inv", rows: "15", disabled: true, value: formatO(mOp) }),
      React.createElement("textarea", { className: "log inv", rows: "15", disabled: true, value: formatM(mMsg) })
    )
  );
}

var domContainer = document.querySelector('#react-container');
ReactDOM.render(React.createElement(App, null), domContainer);
