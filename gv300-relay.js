var datagram = require('dgram');
var Utils = require('util');
var buffer = require('buffer');
var pg = require('pg');
//var cluster = require('cluster');
//var numCPUs = require('os').cpus().length;
var relayPort = 6543;
var connectionInfo =
        {
            user: 'postgres',
            password: 'password',
            database: 'gv300',
            host: 'localhost',
            port: 5432
        };
//--------------------- Main Entry ----------------------
var socket = datagram.createSocket('udp4');
socket.on('error', function(error) {
    console.error(error);
    socket.close();
});
socket.on('message', function(msg, rInfo) {
    console.log('');
    var message = msg.toString();
    Utils.log(message);

    if (rInfo.address === '127.0.0.1') {
        test(message);
    } else {
        dataPacketHandler(message, rInfo, function(result) {
            console.log(result);
        });
    }
});
socket.on('close', function() {
    console.log('Connection Close');
});
socket.on('listening', function() {
    var address = socket.address();
    console.log(process.pid + ' Listening on ' + address.address + ':' + address.port);
});
socket.bind(relayPort);


//function test(msg) {
////    var querystring = require('querystring');
////    var str = '';
////    str = querystring.parse(msg);
//    var buf = new Buffer(msg);
//    var vc = msg.split(',')[2];
//    var b = new Buffer(vc);
//    console.log(b);
//    var sock = datagram.createSocket('udp4');
//    sock.send(buf, 0, buf.length, 30140, '192.168.10.89', function() {
//        console.log(buf.toString());
//        sock.close();
//    });
//
//}
//-----------------------------------------------------------


/**
 * Data hadler function
 * @param {String} message incoming data message
 * @param {Object} remoteInfo Remote Info Object
 * @param {Function} callback Callback Function
 * @returns {dataPacketHandler.packetInformation|Array}
 */
function dataPacketHandler(message, remoteInfo, callback) {
    var tmp = new Array();
    tmp = message.substr(0, message.length - 1).split(',');
    // - DEV -  packet viewer - - - - - -
    for (var i = 0; i < tmp.length; i++) {
        console.log(i + ' : ' + tmp[i]);
    }
    // - - - - - - - - - - - - - - - - - -
    var packetInformation = new Array();
    packetInformation.HEX = parseInt(tmp[4], 16);
    packetInformation.IMEI = tmp[2];
    switch (packetInformation.HEX) {
        case 0x0000:
            // Greeting from Garmin
            Utils.log('Greet Message');
            packetInformation.DRIVER_ID = tmp[18];
            packetInformation.STATUS_ID = tmp[21];
            packetInformation.CHANGED_TIME = tmp[22];
            packetInformation.SEND_TIME = tmp[36];
            updateBoxInfo(packetInformation);
            break;

        case 0x0823:// Set Driver Status Receipt Report
            Utils.log('Got Driver Status Update');

            packetInformation.STATUS_ID = tmp[5];
            packetInformation.CHANGED_TIME = tmp[6];
            packetInformation.SEND_TIME = tmp[17];
            updateStatus(packetInformation);
            break;

        case 0x0813:// A607 Driver ID Update
            Utils.log('Got A607 Driver ID Update');
            packetInformation.DRIVER_ID = tmp[5];
            // Since Driver ID packet not include status id...
            // We prefer to set this to -1 to indicate empty data field
            packetInformation.STATUS_ID = -1;
            packetInformation.CHANGED_TIME = tmp[6];
            packetInformation.SEND_TIME = tmp[17];
            // We can use the same function as greeting message
            updateBoxInfo(packetInformation);
            break;
        case 0x0802:// Set Driver Status List Text Receipt Report
            Utils.log('Got Driver Status List Receipt');
            packetInformation.STATUS_ID = tmp[5];
            packetInformation.RESULT = tmp[6];
            Utils.log((packetInformation.RESULT === 1) ? 'SUCCESS' : 'FAIL');
            break;

        case 0x0261:// Communication Link Status Report
            Utils.log('Got Communication Link Report');
            packetInformation.LINK_STATUS = tmp[6];
            Utils.log('Link Status: ' + (packetInformation.LINK_STATUS === 1 ? 'Established' : 'Lost'));
            break;

//TODO create the handling function of below Garmin Packet IDs
        case 0x0020:// Text Message ACK Report
        case 0x0029:// Canned Response list receipt report
        case 0x002B:// A604 Text Message Receipt Report
        case 0x0032:// Set Canned Response Receipt Report
        case 0x0033:// Delete Canned Response Receipt Reeport
        case 0x0034:// Refresh Canned Response List Text Report
        case 0x0041:// Text Message Status Report 
        case 0x0051:// Set Cannned Message Receipt Report
        case 0x0053:// Delete Canned Message Receipt Report
        case 0x0054:// Refresh Canned Message List Report
        case 0x0111:// Sort Stop List Acknowledgement Report
        case 0x0211:// Stop Status Report
        case 0x0241:// User Interface Text Receipt Report

        case 0x0253:// Request Message Throttling Status Report
        case 0x0260:// Ping Packet Report

        case 0x0803:// Delete Driver Status List Text Receipt Report
        case 0x0804:// Refresh Driver Status List Report
        case 0x0812:// Set Driver ID Receipt Report
        case 0x1001:// Set Speed Limit Alert Receipt Report
        case 0x0251:// Set Message Throttling Response Report

        default:
            console.error('Unhandled Packet Type : 0x' + tmp[4]);
            break;
    }

    if (callback) {
        callback(packetInformation);
    } else {
        return packetInformation;
    }
}
/**
 * Connect a PostgreSQL Server and Query
 * pass the error and result to the callback function
 * @param {String} sql
 * @param {Function} callback
 * @returns {error|ResultSet} error and result of the process
 */
function pg_connectAndQuery(sql, callback) {
    var client = new pg.Client(connectionInfo);
    client.connect(function(error) {
        if (error) {
            client.end();
            callback(error);
            return;
        }
        client.query(sql, function(error, result) {
            client.end();
            callback(error, result);
            return;
        });
    });
}

/**
 * Store greet message data
 * @param {type} input
 * @returns {Object} Error
 */
function updateBoxInfo(input) {
    !input.CHANGED_TIME ? input.CHANGED_TIME = '00000000000000' : '';
    !input.STATUS_ID ? input.STATUS_ID = -1 : '';


    pg_connectAndQuery("SELECT * FROM boxes WHERE imei='" + input.IMEI + "' LIMIT 1;", function(error, r) {
        if (error) {
            return error;
        }
        var sql;
        if (r.rows.length === 0) {

            sql = "INSERT INTO boxes(imei, driver_id, status_id, last_update, last_change) VALUES ('" + input.IMEI + "', '" + input.DRIVER_ID + "', " + input.STATUS_ID + ", to_timestamp('" + input.SEND_TIME + "','YYYYMMDDHH24MISS'),to_timestamp('" + input.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour' );";
            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Driver Info Insert: Failed');
                    console.error(error);
                    return error;
                } else {
                    Utils.log('Driver Info Insert: Success');
                }
            });
        } else {
            sql = "UPDATE boxes SET driver_id='" + input.DRIVER_ID + "', status_id=" + input.STATUS_ID + ", last_update=to_timestamp('" + input.SEND_TIME + "','YYYYMMDDHH24MISS'), last_change=to_timestamp('" + input.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour'  WHERE imei='" + input.IMEI + "';";

            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Driver Info Update: Failed');
                    console.error(error);
                    return error;
                } else {
                    Utils.log('Driver Info Update: Success');
                }
            });
        }
    });
}

/**
 * Store status update data
 * @param {type} input
 * @returns {undefined}
 */
function updateStatus(input) {
    pg_connectAndQuery("SELECT * FROM boxes WHERE imei='" + input.IMEI + "' LIMIT 1;", function(error, r) {
        if (error) {
            return;
        }
        var sql;
        if (r.rows.length === 0) {
            sql = "INSERT INTO boxes(imei, driver_id, status_id, last_update, last_change) VALUES ('" + input.IMEI + "', '" + input.DRIVER_ID + "', " + input.STATUS_ID + ", to_timestamp('" + input.SEND_TIME + "','YYYYMMDDHH24MISS'),to_timestamp('" + input.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour' );";
            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Insert Box Detail: Failed');
                    console.error(error);
                } else {
                    Utils.log('Insert Box Detail: Success');
                }
            });
        } else {
            sql = "UPDATE boxes SET status_id=" + input.STATUS_ID + ", last_update=to_timestamp('" + input.SEND_TIME + "','YYYYMMDDHH24MISS'), last_change=to_timestamp('" + input.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour'  WHERE imei='" + input.IMEI + "';";
            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Update Box Detail: Failed');
                    console.error(error);
                } else {
                    Utils.log('Update Box Detail: Success');
                }
            });
        }
    });

}