'use strict';

const socket = new WebSocket('ws://127.0.0.1:3012');
socket.onerror = e => { console.log('Error: ' + e); };
socket.onclose = e => { console.log('close' + e); };

function Example() {
  // Declare a new state variable, which we'll call "count"
  const [count, setCount] = React.useState(0);
  const [messages, setMessages] = React.useState({});

  React.useEffect(() => {
    socket.onmessage = e => {
      const [[activity, results], t, diff] = JSON.parse(e.data);

      console.log(messages);
      if (diff > 0) {
        messages[activity] = results;
      }

      setMessages({ ...messages });

      console.log(messages);
    };
  });

  console.log(messages);

  return (
    <div>
      <h1>5 hops</h1>
      <ul>
        {Object.keys(messages).map(k => <li>{k}: plain: {messages[k][0]} | weighted: {messages[k][1]}</li>)}
      </ul>
    </div>
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

let domContainer = document.querySelector('#react-container');
ReactDOM.render(<Example />, domContainer);
