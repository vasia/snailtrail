document.querySelector("#epoch").addEventListener("change", function() {
  setEpoch();
});

// if not empty, everything else will be lowlighted
let toHighlight = new Set();

let types = {
  ["Processing"]: "#0b6623",
  ["Spinning"]: "#e48282",
  ["ControlMessage"]: "#4b5f53",
  ["DataMessage"]: "#971757",
  ["Waiting"]: "#FF0000",
  ["Busy"]: "#059dc0",
}

let margins = {
  left: 10,
  top: 10,
  right: 30,
  // bottom: 50,
};

let state = {
  filteredPag: pag,
  lastTransform: d3.zoomIdentity,
  x: null,
  xAxis: null,
  zoom: d3.zoom().scaleExtent([1, 512]).on("zoom", () => {
    state.lastTransform = d3.event.transform;
    update();
  }),
  svgParent: d3.select("#d3").append("svg").attr("id", "graph"),
  svg: null,
  svgXAxis: null,
  activities: null,
};

state.svg = state.svgParent.append("g");
state.svgXAxis = state.svg.append("g").attr("class", "axis axis--x");
state.activities = state.svg.append("g");

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

const hopCheckbox = document.getElementById("hop-highlight");
hopCheckbox.addEventListener("change", function() {
  update();
});

setEpoch();
d3.select(window).on('resize', update);

function setEpoch() {
  let epoch = parseInt(document.getElementById("epoch").value);
  state.filteredPag = pag.filter(p => p.src.e === epoch);

  state.x = d3.scaleLinear().domain([state.filteredPag[0].src.t / 1000000, state.filteredPag[state.filteredPag.length - 1].dst.t / 1000000]);
  state.xAxis = d3.axisBottom(state.x);

  state.activities.selectAll("line").data(state.filteredPag)
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

  state.activities.selectAll("line").data(state.filteredPag)
    .exit()
    .remove();

  state.activities.selectAll("text").data(state.filteredPag)
    .enter()
    .append('text')
    .attr('class', 'label');

  state.activities.selectAll("text").data(state.filteredPag)
    .exit()
    .remove();

  state.svgParent.transition()
    .duration(50)
    .call(state.zoom.transform, d3.zoomIdentity);

  update();
}

function update() {
  let width = (window.innerWidth);

  let domain = [state.filteredPag[0].src.t / 1000000, state.filteredPag[state.filteredPag.length - 1].dst.t / 1000000];
  state.x = d3.scaleLinear().domain(domain);
  state.x.range([margins.left, width - margins.right]);

  state.svgParent.call(state.zoom);
  let xt = state.lastTransform.rescaleX(state.x);

  state.svgXAxis
    .attr("transform", "translate(0," + margins.top + ")")
    .call(state.xAxis.scale(xt));

  state.activities.selectAll("line").data(state.filteredPag)
    .attr('x1', (d) => xt(d.src.t / 1000000))
    .attr('y1', d => 100 * (1 + d.src.w))
    .attr('stroke', d => types[d.type])
    .attr('stroke-width', d => d.src.w !== d.dst.w ? 1.5 : 2)
    .attr('x2', (d) => xt(d.dst.t / 1000000))
    .attr('y2', d => 100 * (1 + d.dst.w))
    .attr('stroke-opacity', d => {
      if (toHighlight.size > 0 && hopCheckbox.checked) {
        return (toHighlight.has(`${d.src.t}${d.dst.t}`) ? 1 : 0.1);
      } else {
        return 1;
      }
    })
    .attr('stroke-dasharray', d => (d.src.w !== d.dst.w) ? "5,5" : "5,0");

  state.activities.selectAll("text").data(state.filteredPag)
    .attr("x", d => xt((d.src.t + d.dst.t) / (2 * 1000000)) - 30)
    .attr("y", d => (((1 + d.src.w) + (1 + d.dst.w)) / 2) * 100 - 10)
    .attr("fill", d => types[d.type])
    .attr("opacity", d => {
      if (toHighlight.size > 0 && hopCheckbox.checked) {
        return (toHighlight.has(`${d.src.t}${d.dst.t}`) ? 1 : 0.1);
      } else {
        return 1;
      }
    })
    .text(genTitle);
}
