var datagram = require('dgram');
var Utils = require('util');
var buffer = require('buffer');
//var pg = require('pg');
//var cluster = require('cluster');
//var numCPUs = require('os').cpus().length;
var relayPort = 6543;



//--------------------- Main Entry ----------------------
var socket = datagram.createSocket('udp4');
socket.on('error', function(error) {
    console.error(error);
    socket.close();
});

socket.on('message', function(msg, info) {
    console.log('-----------------------------------------------');
    console.log('Time    : ' + new Date());
    console.log('From    : ' + info.address + ':' + info.port);
    console.log('Message : ' + msg.toString());
    console.log(msg);
    console.log('-----------------------------------------------');
});

socket.on('close', function() {
    console.log('Connection Close');
});

socket.on('listening', function() {
    var address = socket.address();
    console.log(process.pid + ' Listening on ' + address.address + ':' + address.port);

});
socket.bind(relayPort);


//-----------------------------------------------------------

