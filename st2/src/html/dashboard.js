'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var socket = new WebSocket('ws://127.0.0.1:3012');
socket.onerror = function (e) {
  console.log('Error: ' + e);
};
socket.onclose = function (e) {
  console.log('close' + e);
};

function Example() {
  // Declare a new state variable, which we'll call "count"
  var _React$useState = React.useState(0),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      count = _React$useState2[0],
      setCount = _React$useState2[1];

  var _React$useState3 = React.useState({}),
      _React$useState4 = _slicedToArray(_React$useState3, 2),
      messages = _React$useState4[0],
      setMessages = _React$useState4[1];

  React.useEffect(function () {
    socket.onmessage = function (e) {
      var _JSON$parse = JSON.parse(e.data),
          _JSON$parse2 = _slicedToArray(_JSON$parse, 3),
          _JSON$parse2$ = _slicedToArray(_JSON$parse2[0], 2),
          activity = _JSON$parse2$[0],
          results = _JSON$parse2$[1],
          t = _JSON$parse2[1],
          diff = _JSON$parse2[2];

      console.log(messages);
      if (diff > 0) {
        messages[activity] = results;
      }

      setMessages(Object.assign({}, messages));

      console.log(messages);
    };
  });

  console.log(messages);

  return React.createElement(
    'div',
    null,
    React.createElement(
      'h1',
      null,
      '5 hops'
    ),
    React.createElement(
      'ul',
      null,
      Object.keys(messages).map(function (k) {
        return React.createElement(
          'li',
          null,
          k,
          ': plain: ',
          messages[k][0],
          ' | weighted: ',
          messages[k][1]
        );
      })
    )
  );
}

// class LikeButton extends React.Component {
//   constructor(props) {
//     super(props);
//     this.state = { liked: false };
//   }

//   render() {
//     if (this.state.liked) {
//       return 'You liked this.';
//     }

//     return (
//       <button onClick={() => this.setState({ liked: true })}>
//         Dislike
//       </button>
//     );
//   }
// }

var domContainer = document.querySelector('#react-container');
ReactDOM.render(React.createElement(Example, null), domContainer);