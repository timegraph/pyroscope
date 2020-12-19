// ISC License

// Copyright (c) 2018, Mapbox

// Permission to use, copy, modify, and/or distribute this software for any purpose
// with or without fee is hereby granted, provided that the above copyright notice
// and this permission notice appear in all copies.

// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
// REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
// INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
// OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
// TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
// THIS SOFTWARE.


// This component is based on flamebearer project
//   https://github.com/mapbox/flamebearer


import React from 'react';
import {connect} from 'react-redux';

import {fetchJSON} from '../redux/actions';
// import {FlameBearer} from '../flamebearer';
import MaxNodesSelector from "./MaxNodesSelector";
import clsx from "clsx";

import murmurhash3_32_gc from '../murmur3';
import {numberWithCommas} from '../util/format';


import { withShortcut, ShortcutProvider, ShortcutConsumer } from 'react-keybind'

const PX_PER_LEVEL = 18;
const COLLAPSE_THRESHOLD = 5;
const HIDE_THRESHOLD = 0.5;
const LABEL_THRESHOLD = 20;


function colorBasedOnName(name, filenames, a){
  // const rand = (murmurhash3_32_gc(name) & 100) / 100;
  // const m = 20; //20;
  // const h = 40 + (rand - 0.5) * m;
  // const s = 90;
  // const l = 60;

  // const m = 20; //20;
  // const h = 50 + (rand - 0.5) * m;
  // const s = 35;
  // const l = 41;

  // console.log(`color based on name: ${name}`)
  // console.log(filenames)

  const purple = `hsla(246, 40%, 65%, ${a})` //Purple:
  const blueDark = `hsla(211, 48%, 60%, ${a})` //BlueDark:
  const blueCyan = `hsla(194, 52%, 61%, ${a})` //CyanBlue:
  const yellow = `hsla(34, 65%, 65%, ${a})` //Yellow:
  const green = `hsla(163, 45%, 55%, ${a})` //Green:
  const orange = `hsla(24, 69%, 60%, ${a})` //Orange:
  const red = `hsla(3, 62%, 67%, ${a})` // Red:
  const grey = `hsla(225, 2%, 51%, ${a})` //Grey:

  const items = [
    // red,
    orange,
    yellow,
    green,
    blueCyan,
    blueDark,
    purple,
  ]

  // const darkGreen = `hsla(160, 40%, 21%, ${a})` //Dark green:
  // const darkPurple = `hsla(240, 30%, 29%, ${a})` //puprple:
  // const darkBlue = `hsla(226, 36%, 26%, ${a})` //Dark blue:
  // const darkPink = `hsla(315, 40%, 24%, ${a})` //Pink:
  // const darkYellow = `hsla(62, 29%, 22%, ${a})` //Yellow/mustard:
  // const darkRed = `hsla(10, 41%, 23%, ${a})` //Red:
  //
  // const items = [
  //   darkGreen,
  //   darkPurple,
  //   darkBlue,
  //   darkPink,
  //   darkYellow,
  //   darkRed,
  // ]

  if(name.indexOf('.py') >= 0) {
    let i = filenames.indexOf(name);
    // return items[Math.floor(Math.random() * items.length)];
    return items[murmurhash3_32_gc(name) % items.length];
  } else {
    return grey
  }


  // return `hsla(${h}, ${s}%, ${l}%, ${a})`;
  // return `hsla(191, 35%, 41%, 1)`
  // // return "#48CE73";
  // return "#FEBD46";
  // return "#E3B340";
  // return "#facf5a";
  // return "#f9813a";
  // return "#ffac41";
}

function colorGreyscale(v, a){
  return `rgba(${v}, ${v}, ${v}, ${a})`;
}

// Don't move this is FlameBearer
export function deltaDiff(levels) {
  for (const level of levels) {
    let prev = 0;
    for (let i = 0; i < level.length; i += 3) {
      level[i] += prev;
      prev = level[i] + level[i + 1];
    }
  }
}


class FlameGraphRenderer extends React.Component {
  constructor (){
    super();
    this.state = {
      highlightStyle: {display: 'none'},
      tooltipStyle: {display: 'none'},
      resetStyle: {visibility: 'hidden'},
    };
    this.canvasRef = React.createRef();
    this.tooltipRef = React.createRef();
    // this.getFilenameFromStackTrace = this.getFilenameFromStackTrace.bind(this);
  }

  componentDidMount() {
    // this.maybeFetchJSON();

    this.canvas = this.canvasRef.current;
    this.ctx = this.canvas.getContext('2d');
    this.topLevel = 0; //Todo: could be a constant
    this.selectedLevel = 0;
    this.rangeMin = 0;
    this.rangeMax = 1;
    this.query = "";

    window.addEventListener('resize', this.resizeHandler);

    this.props.shortcut.registerShortcut(this.reset, ['escape'], 'Reset', 'Reset Flamegraph View');
  }

  componentDidUpdate(prevProps) {

    // this.maybeFetchJSON()
    // if(this.props.flamebearer && prevProps.flamebearer != this.props.flamebearer) {
    //   this.updateData(this.props.flamebearer);
    // }
  }

  // maybeFetchJSON(){
  //   let url = this.props.renderURL;
  //   if(this.lastRequestedURL != url) {
  //     this.lastRequestedURL = url
  //     this.props.fetchJSON(url);
  //   }
  // }

  roundRect(ctx, x, y, w, h, radius) {
    radius = Math.min(w/2, radius);
    if (radius < 1) {
      return ctx.rect(x,y,w,h);
    }
    var r = x + w;
    var b = y + h;
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.lineTo(r - radius, y);
    ctx.quadraticCurveTo(r, y, r, y + radius);
    ctx.lineTo(r, y + h - radius);
    ctx.quadraticCurveTo(r, b, r - radius, b);
    ctx.lineTo(x + radius, b);
    ctx.quadraticCurveTo(x, b, x, b - radius);
    ctx.lineTo(x, y + radius);
    ctx.quadraticCurveTo(x, y, x + radius, y);
  }

  updateZoom(i, j) {
    if (!isNaN(i) && !isNaN(j)) {
      this.selectedLevel = i;
      this.topLevel = 0;
      this.rangeMin = this.levels[i][j] / this.numTicks;
      this.rangeMax = (this.levels[i][j] + this.levels[i][j + 1]) / this.numTicks;
    } else {
      this.selectedLevel = 0;
      this.topLevel = 0;
      this.rangeMin = 0;
      this.rangeMax = 1;
    }
    this.updateResetStyle();
  }

  updateData() {
    if (!this.props.flamebearer) { return };

    let { names, levels, numTicks } = this.props.flamebearer;
    this.names = names;
    let allFilenames = this.names.map((stackTrace) => {
      return this.getFilenameFromStackTrace(stackTrace)
    })
    this.filenames = [...new Set(allFilenames)];
    this.levels = levels;
    this.numTicks = numTicks;
    this.renderCanvas();
  }

  // binary search of a block in a stack level
  binarySearchLevel(x, level) {
    let i = 0;
    let j = level.length - 3;
    while (i <= j) {
      const m = 3 * ((i / 3 + j / 3) >> 1);
      const x0 = this.tickToX(level[m]);
      const x1 = this.tickToX(level[m] + level[m + 1]);
      if (x0 <= x && x1 >= x) {
        return x1 - x0 > COLLAPSE_THRESHOLD ? m : -1;
      }
      if (x0 > x) {
        j = m - 3;
      } else {
        i = m + 3;
      }
    }
    return -1;
  }

  getFilenameFromStackTrace(stackTrace) {
    if(stackTrace.length == 0) {
      return stackTrace
    } else {
      let fullStackGroups = stackTrace.match(/^(?<path>(.*\/)*)(?<filename>.*\.py+)(?<line_info>.*)$/)
      if(fullStackGroups) {
        return fullStackGroups.groups.filename
      } else {
        return stackTrace
      }
    }
  }

  updateResetStyle = () => {
    // const emptyQuery = this.query === "";
    const topLevelSelected = this.selectedLevel === 0;
    this.setState({
      resetStyle: { visibility: topLevelSelected ? 'hidden' : 'visible' }
    })
  }

  handleSearchChange = (e) => {
    this.query = e.target.value;
    this.updateResetStyle();
    this.renderCanvas();
  }

  reset = () => {
    this.updateZoom(0, 0);
    this.renderCanvas();
  }

  xyToBar = (x, y) => {
    const i = Math.floor(y / PX_PER_LEVEL) + this.topLevel;
    if(i >= 0 && i < this.levels.length) {
      const j = this.binarySearchLevel(x, this.levels[i]);
      return { i, j };
    }
    return {i:0,j:0};
  }

  clickHandler = (e) => {
    const { i, j } = this.xyToBar(e.nativeEvent.offsetX, e.nativeEvent.offsetY);
    if (j === -1) return;

    this.updateZoom(i, j);
    this.renderCanvas();
    this.mouseOutHandler();
  }

  resizeHandler = () => {
    clearTimeout(this.resizeFinish);
    this.resizeFinish = setTimeout(this.render, 100);
  }

  tickToX = (i) => {
    return (i - this.numTicks * this.rangeMin) * this.pxPerTick;
  }

  renderCanvas = () => {
    if(!this.names) {
      return;
    }

    let { names, levels, numTicks } = this;
    this.graphWidth = this.canvas.width = this.canvas.clientWidth;
    this.pxPerTick = this.graphWidth / numTicks / (this.rangeMax - this.rangeMin);
    this.canvas.height = PX_PER_LEVEL * (levels.length - this.topLevel);
    this.canvas.style.height = this.canvas.height + 'px';

    if (devicePixelRatio > 1) {
      this.canvas.width *= 2;
      this.canvas.height *= 2;
      this.ctx.scale(2, 2);
    }


    this.ctx.textBaseline = 'middle';
    this.ctx.font = '300 12px system-ui, -apple-system, "Segoe UI", "Roboto", "Ubuntu", "Cantarell", "Noto Sans", sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji"';

    // i = level
    for (let i = 0; i < levels.length - this.topLevel; i++) {
      const level = levels[this.topLevel + i];


      for (let j = 0; j < level.length; j += 3) {
        // j = 0: x start of bar
        // j = 1: width of bar
        // j = 2: position in the main index

        const barIndex = level[j];
        const x = this.tickToX(barIndex);
        const y = i * PX_PER_LEVEL;
        let numBarTicks = level[j + 1];

        // For this particular bar, there is a match
        const inQuery = this.query && (names[level[j + 2]].indexOf(this.query) >= 0) || false;

        // merge very small blocks into big "collapsed" ones for performance
        const collapsed = numBarTicks * this.pxPerTick <= COLLAPSE_THRESHOLD;
        // const collapsed = false;
        if (collapsed) {
            while (
                j < level.length - 3 &&
                barIndex + numBarTicks === level[j + 3] &&
                level[j + 4] * this.pxPerTick <= COLLAPSE_THRESHOLD &&
                (inQuery === (this.query && (names[level[j + 5]].indexOf(this.query) >= 0) || false))
            ) {
                j += 3;
                numBarTicks += level[j + 1];
            }
        }
        // ticks are samples
        const sw = numBarTicks * this.pxPerTick - (collapsed ? 0 : 0.5);
        const sh = PX_PER_LEVEL - 0.5;

        // if (x < -1 || x + sw > this.graphWidth + 1 || sw < HIDE_THRESHOLD) continue;

        this.ctx.beginPath();
        this.roundRect(this.ctx, x, y, sw, sh, 3);

        const ratio = numBarTicks / numTicks;

        const a = this.selectedLevel > i ? 0.33 : 1;
        if (!collapsed) {
          this.ctx.fillStyle = inQuery ? '#48CE73' : colorBasedOnName(this.getFilenameFromStackTrace(names[level[j + 2]]), this.filenames, a);
        } else {
          this.ctx.fillStyle = inQuery ? '#48CE73' : colorGreyscale(200, 0.66);
        }
        this.ctx.fill();

        if (!collapsed && sw >= LABEL_THRESHOLD) {

          const percent = Math.round(10000 * ratio) / 100;
          const name = `${names[level[j + 2]]} (${percent}%, ${numberWithCommas(numBarTicks)} samples)`;

          this.ctx.save();
          this.ctx.clip();
          this.ctx.fillStyle = 'black';
          this.ctx.fillText(name, Math.round(Math.max(x, 0) + 3), y + sh / 2);
          this.ctx.restore();
        }
      }
    }
  }
  mouseMoveHandler = (e) => {
    const { i, j } = this.xyToBar(e.nativeEvent.offsetX, e.nativeEvent.offsetY);

    if (j === -1 || e.nativeEvent.offsetX < 0 || e.nativeEvent.offsetX > this.graphWidth) {
      this.mouseOutHandler();
      return;
    }

    this.canvas.style.cursor = 'pointer';

    const level = this.levels[i];
    const x = Math.max(this.tickToX(level[j]), 0);
    const y = (i - this.topLevel) * PX_PER_LEVEL;
    const sw = Math.min(this.tickToX(level[j] + level[j + 1]) - x, this.graphWidth);

    const tooltipEl = this.tooltipRef.current;
    const numBarTicks = level[j + 1];
    const percent = Math.round(10000 * numBarTicks / this.numTicks) / 100;
    this.setState({
      highlightStyle: {
        display: 'block',
        left:    (this.canvas.offsetLeft + x) + 'px',
        top:     (this.canvas.offsetTop + y) + 'px',
        width:   sw + 'px',
        height:  PX_PER_LEVEL + 'px',
      },
      tooltipStyle: {
        display: 'block',
        left: (Math.min(e.nativeEvent.offsetX + 15 + tooltipEl.clientWidth, this.graphWidth) - tooltipEl.clientWidth) + 'px',
        top: (this.canvas.offsetTop + e.nativeEvent.offsetY + 12) + 'px',
      },
      tooltipText1: this.names[level[j + 2]],
      tooltipText2: `${percent}%, ${numberWithCommas(numBarTicks)} samples`,
    });
  }

  mouseOutHandler = () => {
    this.canvas.style.cursor = '';
    this.setState({
      highlightStyle : {
        display: 'none',
      },
      tooltipStyle : {
        display: 'none',
      }
    })
  }

  render() {
    this.updateData();

    return (
      <div className="canvas-renderer">
        <div className="canvas-container">
          <div className="navbar-2">
            <input name="flamegraph-search" placeholder="Search..." onChange={this.handleSearchChange} />
            &nbsp;
            <button className={clsx('btn')} style={this.state.resetStyle} id="reset" onClick={this.reset}>Reset View</button>
            <div className="navbar-space-filler"></div>
            <MaxNodesSelector />
          </div>
          <canvas className="flamegraph-canvas" height="0" ref={this.canvasRef} onClick={this.clickHandler} onMouseMove={this.mouseMoveHandler} onMouseOut={this.mouseOutHandler}></canvas>
        </div>
        <div style={this.state.highlightStyle}></div>
        <div className="flamegraph-tooltip" ref={this.tooltipRef} style={this.state.tooltipStyle}>
          <div className="flamegraph-tooltip-name">{this.state.tooltipText1}</div>
          <div>{this.state.tooltipText2}</div>
        </div>
      </div>
    );
  }

}

export default connect(
  (x) => x,
  { fetchJSON }
)(withShortcut(FlameGraphRenderer));
