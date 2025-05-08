const WebSocket = require('ws');
const url = 'ws://localhost:8082/activity';

const socket = new WebSocket(url);

socket.onopen = () => {
    console.log('Connected to WebSocket server');

    // Send initial message
    const message = JSON.stringify({
        command_type: "get-drivers",
        page: 1
    });
    socket.send(message);

    // Keep connection alive with periodic pings
    setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) {
            socket.ping();
        }
    }, 30000);
};

socket.onmessage = (event) => {
    console.log('Received:', event.data);
};

socket.onerror = (error) => {
    console.error('WebSocket Error:', error);
};

socket.onclose = (event) => {
    console.log('Connection closed', event.code, event.reason);
};

// Keep process alive
process.stdin.resume();