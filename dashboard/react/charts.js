const khopsChart = {
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

const weightedKhopsChart = {
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

const activityCountChart = {
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
      "field": "ac",
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

const activityDurationChart = {
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
      "field": "at",
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

const msgsCountChart = {
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
      "field": "ac",
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

const msgsDurationChart = {
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
      "field": "at",
    },
    "color": {
      "field": "a",
      "type": "nominal",
      "legend": { "title": "activity type" }
    }
  }
};

const recordSentChart = {
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
      "field": "rc",
    }
  }
};

const recordProcessedChart = {
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
      "field": "rc",
    }
  }
};

const types = {
  ["Processing"]: "#0b6623",
  ["Spinning"]: "#e48282",
  ["ControlMessage"]: "#4b5f53",
  ["DataMessage"]: "#971757",
  ["Waiting"]: "#FF0000",
  ["Busy"]: "#059dc0",
};

const margins = {
  left: 10,
  top: 10,
  right: 30,
};

let pag = [];

const genTitle = d => `${d.type} ${d.o === 0 ? '' : 'Op' + d.o + ' '}${d.l > 0 ? '(' + d.l + ')' : ''}`;

const tooltip = d3.select("body").append("div")
  .attr("class", "tooltip")
  .style("opacity", 0);

const makeTooltip = d => `
    w${d.src.w}, ${d.src.t / 1000000} -> w${d.dst.w}, ${d.dst.t / 1000000} <br>
    type: ${d.type} <br>
    operator: ${d.o} <br>
    length: ${d.l} <br>
    traversal: ${d.tr} <br>`;

let pagState = {};
const toHighlight = new Set();

function setPAGEpoch() {
  if (pag.length < 2) {
    return;
  }

  pagState.x = d3.scaleLinear().domain([pag[0].src.t / 1000000, pag[pag.length - 1].dst.t / 1000000]);
  pagState.xAxis = d3.axisBottom(pagState.x);

  pagState.activities.selectAll("line").data(pag)
    .enter()
    .append('line')
    .attr('class', 'activity')
    .on("mouseover", d => {
      tooltip.transition()
        .style("opacity", 1);
      tooltip.html(makeTooltip(d))
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY + 20) + "px");
    })
    .on("mouseout", d => tooltip.transition().style("opacity", 0));

  pagState.activities.selectAll("line").data(pag)
    .exit()
    .remove();

  pagState.activities.selectAll("text").data(pag)
    .enter()
    .append('text')
    .attr('class', 'label');

  pagState.activities.selectAll("text").data(pag)
    .exit()
    .remove();

  pagState.svgParent.transition()
    .duration(50)
    .call(pagState.zoom.transform, d3.zoomIdentity);
}

function updatePAG() {
  if (pag.length < 2) {
    return;
  }

  let highlight = document.getElementById("hop-highlight").checked;
  let width = (window.innerWidth);

  let domain = [pag[0].src.t / 1000000, pag[pag.length - 1].dst.t / 1000000];
  pagState.x = d3.scaleLinear().domain(domain);
  pagState.x.range([margins.left, width - margins.right]);

  pagState.svgParent.call(pagState.zoom);
  let xt = pagState.lastTransform.rescaleX(pagState.x);

  pagState.svgXAxis
    .attr("transform", "translate(0," + margins.top + ")")
    .call(pagState.xAxis.scale(xt));

  pagState.activities.selectAll("line").data(pag)
    .attr('x1', (d) => xt(d.src.t / 1000000))
    .attr('y1', d => 100 * (1 + d.src.w))
    .attr('stroke', d => types[d.type])
    .attr('stroke-width', d => d.src.w !== d.dst.w ? 1.5 : 2)
    .attr('x2', (d) => xt(d.dst.t / 1000000))
    .attr('y2', d => 100 * (1 + d.dst.w))
    .attr('stroke-opacity', d => {
      if (toHighlight.size > 0 && highlight) {
        return (toHighlight.has(`${d.src.t}${d.dst.t}`) ? 1 : 0.1);
      } else {
        return 1;
      }
    })
    .attr('stroke-dasharray', d => (d.src.w !== d.dst.w) ? "5,5" : "5,0");

  pagState.activities.selectAll("text").data(pag)
    .attr("x", d => xt((d.src.t + d.dst.t) / (2 * 1000000)) - 30)
    .attr("y", d => (((1 + d.src.w) + (1 + d.dst.w)) / 2) * 100 - 10)
    .attr("fill", d => types[d.type])
    .attr("opacity", d => {
      if (toHighlight.size > 0 && highlight) {
        return (toHighlight.has(`${d.src.t}${d.dst.t}`) ? 1 : 0.1);
      } else {
        return 1;
      }
    })
    .text(genTitle);
}

const socket = new WebSocket('ws:127.0.0.1:3012');

function App() {
  const [epoch, setEpoch] = React.useState(1);
  const [highlight, setHighlight] = React.useState(true);
  const [showWaiting, setShowWaiting] = React.useState(true);
  const [splitWorker, setSplitWorker] = React.useState(false);

  React.useEffect(() => {
    const svgParent = d3.select("#d3").append("svg").attr("id", "graph");
    const svg = svgParent.append("g");
    const svgXAxis = svg.append("g").attr("class", "axis axis--x");
    const activities = svg.append("g");
    pagState = {
      epoch,
      lastTransform: d3.zoomIdentity,
      x: null,
      xAxis: null,
      zoom: d3.zoom().scaleExtent([1, 512]).on("zoom", () => {
        pagState.lastTransform = d3.event.transform;
        updatePAG();
      }),
      svgParent,
      svg,
      svgXAxis,
      activities,
    };

    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "ALL") {
        payload.forEach(d => toHighlight.add(`${d[0]}${d[1]}`));
        updatePAG();
      } else if (type == "PAG") {
        pag = payload;
        setPAGEpoch(pagState);
      }
    });

    socket.send(JSON.stringify({ type: 'PAG', epoch }));
    socket.send(JSON.stringify({ type: 'AGG', epoch }));
    socket.send(JSON.stringify({ type: 'ALL', epoch }));
    socket.send(JSON.stringify({ type: 'MET', epoch }));
    d3.select(window).on('resize', updatePAG());

    socket.send(JSON.stringify({ type: 'INV' }));
    setInterval(() => { socket.send(JSON.stringify({ type: 'INV' })); }, 5000);
  }, []);

  const epochUpdate = e => {
    let epoch = parseInt(e.target.value);
    setEpoch(epoch || '');
    if (epoch) {
      socket.send(JSON.stringify({ type: 'PAG', epoch }));
      socket.send(JSON.stringify({ type: 'AGG', epoch }));
      socket.send(JSON.stringify({ type: 'ALL', epoch }));
      socket.send(JSON.stringify({ type: 'MET', epoch }));
      pagState = { ...pagState, epoch };
    }
  };

  const highlightUpdate = e => {
    updatePAG();
    setHighlight(e.target.checked);
  };

  return (
    <div>
      <h1 style={{ textAlign: "center" }}> ST2 Dashboard</h1>
      <h1>PAG Viz</h1>
      <div id="d3"></div>
      <div className="viz-settings">
        <b style={{ marginRight: "6px" }}>Highlight hops: </b>
        <input id="hop-highlight" type="checkbox" style={{ marginRight: "24px" }} checked={highlight} onChange={highlightUpdate}></input>
        <b style={{ marginRight: "6px" }}>Epoch: </b>
        <input id="epoch" type="text" value={epoch} onChange={epochUpdate}></input>
      </div>
      <div style={{ flex: "0 1 auto" }}>
        <b style={{ marginRight: "6px" }}>Show waiting/busy: </b>
        <input type="checkbox" style={{ marginRight: "24px" }} checked={showWaiting} onChange={e => setShowWaiting(e.target.checked)}></input>
        <b style={{ marginRight: "6px" }}>Split by worker: </b>
        <input type="checkbox" checked={splitWorker} onChange={e => setSplitWorker(e.target.checked)}></input>
      </div>
      <div style={{ display: "flex", flexFlow: "row wrap" }}>
        <KHops epoch={epoch} showWaiting={showWaiting} splitWorker={splitWorker}></KHops>
        <ActivityMetrics epoch={epoch} showWaiting={showWaiting} splitWorker={splitWorker}></ActivityMetrics>
        <CrossMetrics epoch={epoch} showWaiting={showWaiting} splitWorker={splitWorker}></CrossMetrics>
        <RecordMetrics epoch={epoch} showWaiting={showWaiting} splitWorker={splitWorker}></RecordMetrics>
      </div>
      <Invariants></Invariants>
    </div >
  );
}

const prepper = (data, sumKey, showWaiting, splitWorker) => {
  const filtered = data.filter(({ a }) => showWaiting || (!a.startsWith("Wait") && !a.startsWith("Bus")));

  if (splitWorker) {
    return filtered;
  } else {
    return filtered.reduce((acc, d) => {
      let idx = acc.findIndex(x => x.a === d.a);
      if (idx > -1) {
        acc[idx][sumKey] += d[sumKey];
        return acc;
      } else {
        return [...acc, { a: d.a, [sumKey]: d[sumKey], wf: "all", cw: "all" }];
      }
    }, []);
  }
};

function RecordMetrics({ epoch, showWaiting, splitWorker }) {
  // Plot 1: # records sent by each worker to each worker
  const [p1, setP1] = React.useState(undefined);
  // Plot 2: # records processed by each worker
  const [p2, setP2] = React.useState(undefined);
  const [metricsData, setMetricsData] = React.useState([]);

  const p1Ref = React.useRef(null);
  const p2Ref = React.useRef(null);

  React.useEffect(() => {
    vegaEmbed(p1Ref.current, recordSentChart, { actions: false }).then(res => setP1(res.view));
    vegaEmbed(p2Ref.current, recordProcessedChart, { actions: false }).then(res => setP2(res.view));

    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "MET") { setMetricsData(payload); }
    });
  }, []);

  React.useEffect(() => {
    if (p1) {
      const dataMsgs = metricsData
        .filter(d => d.a.startsWith("Dat"))
        .map(d => ({ ...d, cw: [d.wf, d.wt] }));
      p1.change('table', vega.changeset().remove(() => true).insert(prepper(dataMsgs, "rc", showWaiting, splitWorker))).run();
    }

    if (p2) {
      const procMsgs = metricsData.filter(d => d.a.startsWith("Proc"));
      p2.change('table', vega.changeset().remove(() => true).insert(prepper(procMsgs, "rc", showWaiting, splitWorker))).run();
    }
  });

  return (
    <div>
      <h1 style={{ marginRight: "18px" }}>Record Metrics (for epoch {epoch})</h1>
      <div style={{ display: "flex", flexFlow: "row wrap" }}>
        <div>
          <h2>Records Sent</h2>
          <div ref={p1Ref}></div>
        </div>
        <div>
          <h2>Records Processed</h2>
          <div ref={p2Ref}></div>
        </div>
      </div>
    </div>
  );
}


function CrossMetrics({ epoch, showWaiting, splitWorker }) {
  // Plot 1
  // # data messages between workers
  // # control messages between workers
  const [p1, setP1] = React.useState(undefined);
  // Plot 2
  // t data messages between workers
  // t control messages between workers
  const [p2, setP2] = React.useState(undefined);
  const [metricsData, setMetricsData] = React.useState([]);

  const p1Ref = React.useRef(null);
  const p2Ref = React.useRef(null);

  React.useEffect(() => {
    vegaEmbed(p1Ref.current, msgsCountChart, { actions: false }).then(res => setP1(res.view));
    vegaEmbed(p2Ref.current, msgsDurationChart, { actions: false }).then(res => setP2(res.view));

    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "MET") {
        setMetricsData(payload
          .filter(d => d.a.startsWith("Dat") || d.a.startsWith("Con"))
          .map(d => ({ ...d, cw: [d.wf, d.wt] })));
      }
    });
  }, []);

  React.useEffect(() => {
    if (p1) {
      p1.change('table', vega.changeset().remove(() => true).insert(prepper(metricsData, "ac", showWaiting, splitWorker))).run();
    }

    if (p2) {
      p2.change('table', vega.changeset().remove(() => true).insert(prepper(metricsData, "at", showWaiting, splitWorker))).run();
    }
  });

  return (
    <div>
      <h1 style={{ marginRight: "18px" }}>Cross Metrics (for epoch {epoch})</h1>
      <div style={{ display: "flex", flexFlow: "row wrap" }}>
        <div>
          <h2>Messages Count</h2>
          <div ref={p1Ref}></div>
        </div>
        <div>
          <h2>Messages Duration</h2>
          <div ref={p2Ref}></div>
        </div>
      </div>
    </div>
  );
}


function ActivityMetrics({ epoch, showWaiting, splitWorker }) {
  const [aC, setAC] = React.useState(undefined);
  const [aD, setAD] = React.useState(undefined);
  const [metricsData, setMetricsData] = React.useState([]);

  const aCRef = React.useRef(null);
  const aDRef = React.useRef(null);


  React.useEffect(() => {
    vegaEmbed(aCRef.current, activityCountChart, { actions: false }).then(res => setAC(res.view));
    vegaEmbed(aDRef.current, activityDurationChart, { actions: false }).then(res => setAD(res.view));

    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "MET") { setMetricsData(payload); }
    });
  }, []);

  React.useEffect(() => {
    if (aC) {
      aC.change('table', vega.changeset().remove(() => true).insert(prepper(metricsData, "ac", showWaiting, splitWorker))).run();
    }

    if (aD) {
      aD.change('table', vega.changeset().remove(() => true).insert(prepper(metricsData, "at", showWaiting, splitWorker))).run();
    }
  });

  return (
    <div>
      <h1 style={{ marginRight: "18px" }}>Activity Metrics (for epoch {epoch})</h1>
      <div style={{ display: "flex", flexFlow: "row wrap" }}>
        <div>
          <h2>Activity Count</h2>
          <div ref={aCRef}></div>
        </div>
        <div>
          <h2>Activity Duration</h2>
          <div ref={aDRef}></div>
        </div>
      </div>
    </div>
  );
}

function KHops({ epoch, showWaiting, splitWorker }) {
  const [vis, setVis] = React.useState(undefined);
  const [wVis, setWVis] = React.useState(undefined);
  const [visData, setVisData] = React.useState([]);

  const visRef = React.useRef(null);
  const wVisRef = React.useRef(null);

  const khopsPrepper = (data, sumKey, showWaiting, splitWorker) => {
    const filtered = data.filter(({ a }) => showWaiting || (!a.startsWith("Wait") && !a.startsWith("Bus")));

    if (splitWorker) {
      return filtered;
    } else {
      return filtered.reduce((acc, d) => {
        let idx = acc.findIndex(x => x.ca === d.ca);
        if (idx > -1) {
          acc[idx][sumKey] += d[sumKey];
          return acc;
        } else {
          return [...acc, { ca: d.a, [sumKey]: d[sumKey] }];
        }
      }, []);
    }
  };

  React.useEffect(() => {
    vegaEmbed(visRef.current, khopsChart, { actions: false }).then(res => setVis(res.view));
    vegaEmbed(wVisRef.current, weightedKhopsChart, { actions: false }).then(res => setWVis(res.view));

    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "AGG") { setVisData(payload.map(d => ({ ...d, ca: [d.a, d.wf] }))); }
    });
  }, []);

  React.useEffect(() => {
    if (vis) {
      vis.change('table', vega.changeset().remove(() => true).insert(khopsPrepper(visData, "ac", showWaiting, splitWorker))).run();
    }

    if (wVis) {
      wVis.change('table', vega.changeset().remove(() => true).insert(khopsPrepper(visData, "wac", showWaiting, splitWorker))).run();
    }
  });

  return (
    <div>
      <h1 style={{ marginRight: "18px" }}>K-Hops (up to epoch {epoch})</h1>
      <div style={{ display: "flex", flexFlow: "row wrap" }}>
        <div>
          <h2>Unweighted</h2>
          <div ref={visRef}></div>
        </div>
        <div>
          <h2>Weighted</h2>
          <div ref={wVisRef}></div>
        </div>
      </div>
    </div>
  );
}

const formatE = e => {
  if (e.length > 0) {
    return e
      .sort((a, b) => ((ns(b.to.timestamp) - ns(b.from.timestamp)) - (ns(a.to.timestamp) - ns(a.from.timestamp))))
      .map(({ from, to }) => {
        const duration = ((ns(to.timestamp) - ns(from.timestamp)) / 1000000).toFixed(2);
        return `Epoch ${from.epoch} took ${duration}ms.`;
      })
      .join("\n");
  } else {
    return "No invariants violated.";
  }
};

const ns = ts => ts.nanos + ts.secs * 1000000000;

const formatO = e => {
  if (e.length > 0) {
    return e
      .sort((a, b) => ((ns(b.from.destination.timestamp) - ns(b.from.source.timestamp)) - (ns(a.from.destination.timestamp) - ns(a.from.source.timestamp))))
      .map(({ from, to }) => {
        const duration = ((ns(from.destination.timestamp) - ns(from.source.timestamp)) / 1000000).toFixed(2);
        return `Epoch ${from.source.epoch} | w${from.source.worker_id} @ ${ns(from.source.timestamp)}: Operator ${from.operator_id} (${from.edge_type}) took ${duration}ms.`;
      })
      .join("\n");
  } else {
    return "No invariants violated.";
  }

};

const formatM = e => {
  if (e.length > 0) {
    return e
      .sort((a, b) => ((ns(b.msg.destination.timestamp) - ns(b.msg.source.timestamp)) - (ns(a.msg.destination.timestamp) - ns(a.msg.source.timestamp))))
      .map(({ msg }) => {
        const duration = ((ns(msg.destination.timestamp) - ns(msg.source.timestamp)) / 1000000).toFixed(2);
        return `Epoch ${msg.source.epoch} | w${msg.source.worker_id} @ ${ns(msg.source.timestamp)} -> w${msg.destination.worker_id} @ ${ns(msg.destination.timestamp)}: ${msg.edge_type} took ${duration}ms.`;
      })
      .join("\n");
  } else {
    return "No invariants violated.";
  }
};

function Invariants() {
  const [mEpoch, setMEpoch] = React.useState([]);
  const [mOp, setMOp] = React.useState([]);
  const [mMsg, setMMsg] = React.useState([]);
  const [mE, setME] = React.useState(null);
  const [mO, setMO] = React.useState(null);
  const [mM, setMM] = React.useState(null);

  React.useEffect(() => {
    socket.addEventListener("message", e => {
      const { type, payload } = JSON.parse(e.data);
      if (type === "INV") {
        let e = [];
        let o = [];
        let m = [];

        payload.forEach(p => {
          if (p["Epoch"]) {
            if (!mE) {
              setME(`${p["Epoch"].max / 1000000}`);
            }
            e.push(p["Epoch"]);
          } else if (p["Operator"]) {
            if (!mO) {
              setMO(`${p["Operator"].max / 1000000}`);
            }
            o.push(p["Operator"]);
          } else if (p["Message"]) {
            if (!mM) {
              setMM(`${p["Message"].max / 1000000}`);
            }
            m.push(p["Message"]);
          }
        });

        e.length && setMEpoch(prev => prev.concat(e));
        o.length && setMOp(prev => prev.concat(o));
        m.length && setMMsg(prev => prev.concat(m));
      }
    });
  }, []);


  return (
    <div>
      <h1>Invariant Violations (over all epochs)</h1>
      <div style={{ display: "flex", flexFlow: "row wrap", width: "100%" }}>
        <h2 className="inv">Epoch Duration {mE && `(max: ${mE}ms)`}</h2>
        <h2 className="inv">Operator Duration {mO && `(max: ${mO}ms)`}</h2>
        <h2 className="inv">Message Duration {mM && `(max: ${mM}ms)`}</h2>
      </div>
      <div style={{ display: "flex", flexFlow: "row wrap", width: "100%" }}>
        <textarea className="log inv" rows="15" disabled value={formatE(mEpoch)}></textarea>
        <textarea className="log inv" rows="15" disabled value={formatO(mOp)}></textarea>
        <textarea className="log inv" rows="15" disabled value={formatM(mMsg)}></textarea>
      </div>
    </div >
  );
}


let domContainer = document.querySelector('#react-container');
ReactDOM.render(<App />, domContainer);
