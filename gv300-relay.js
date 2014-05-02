var datagram = require('dgram');
var Utils = require('util');
var buffer = require('buffer');
var pg = require('pg');
//var cluster = require('cluster');
//var numCPUs = require('os').cpus().length;
var relayPort = 6543;
var ConnectionInfo = {
    user: 'postgres',
    password: 'password',
    database: 'gv300',
    host: 'localhost',
    port: 5432
};
var GatewayInfo = {
    address: '58.64.30.187',
    port: 30170
};
var dataIDArray = new Array();
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
    increaseRecvData(msg);
    dataPacketHandler(message, rInfo, function(result) {
        //console.log(result);
    });
});
socket.on('close', function() {
    console.log('Connection Close');
});
socket.on('listening', function() {
    var address = socket.address();
    Utils.log('PID:' + process.pid + ' : Listening on ' + address.address + ':' + address.port);
});
socket.bind(relayPort);
/**
 * Log the message into the database table name `logs`
 * @param {type} imei
 * @param {type} message
 * @returns {undefined}
 */
function pg_log(imei, message) {
    var sql = "INSERT INTO logs(imei, data)VALUES ('" + imei + "',  '" + message + "');";
    pg_connectAndQuery(sql, function(message, result) {
    });
}

/**
 * Data hadler function
 * @param {String} message incoming data message
 * @param {Object} remoteInfo Remote Info Object
 * @param {Function} callback Callback Function
 * @returns {packetInfo.packetInformation|Array}
 */
function dataPacketHandler(message, remoteInfo, callback) {
    var tmp = new Array();
    tmp = message.substr(0, message.length - 1).split(',');
    var packetInfo = new Array();
    packetInfo.HEX = parseInt(tmp[4], 16);
    packetInfo.IMEI = tmp[2];
    pg_log(packetInfo.IMEI, message);
    // - - DEV - - packet viewer - - - - -
    for (var i = 0; i < tmp.length; i++) {
        console.log(i + ' : ' + tmp[i]);
    }
    // - - - - - - - - - - - - - - - - - -

    if (tmp[0] === '+BUFF:GTFMI') {
        // Currently ignore the buffered data
        return;
    } else if (tmp[0] === '+ACK:GTFMI' && dataIDArray[packetInfo.IMEI]) {
        console.log('ACK to: ' + dataIDArray[packetInfo.IMEI].packet_id);
        (dataIDArray[packetInfo.IMEI].packet_id)++;
        deleteSentQueuedCommand(dataIDArray[packetInfo.IMEI].sql_id);
        sendFirstQueuedCommand(packetInfo.IMEI);
    } else if (tmp[0] === "+RESP:GTFMI") {
        switch (packetInfo.HEX) {
            case 0x0000:
                // Fixed-Time Reporting
                Utils.log('Got Fixed Time Report');
                packetInfo.DRIVER_ID = tmp[18];
                packetInfo.DRIVER_ID_CHANGED_TIME = tmp[19];
                packetInfo.STATUS_ID = tmp[21];
                packetInfo.STATUS_ID_CHANGED_TIME = tmp[22];
                packetInfo.SEND_TIME = tmp[36];
                //----- Unuse -----
                packetInfo.GPS_FIX = tmp[6];
                packetInfo.SPEED = tmp[7]; //kmph
                packetInfo.AZIMUTH = tmp[8];
                packetInfo.ALTITUDE = tmp[9]; //m
                packetInfo.LONGITUDE = tmp[10];
                packetInfo.LATITUDE = tmp[11];
                packetInfo.GPS_UTC_TIME = tmp[12];
                packetInfo.ETA_ID = tmp[13];
                packetInfo.ETA_TIME = tmp[14];
                packetInfo.DIST_TO_DEST = tmp[15]; // m
                packetInfo.DEST_LONGITUDE = tmp[16];
                packetInfo.DEST_LATITUDE = tmp[17];
                packetInfo.DRIVER_INDEX = tmp[20];
                updateBoxInfo(packetInfo);
                // ------- sending queued command
                if (!dataIDArray[packetInfo.IMEI]) {
                    dataIDArray[packetInfo.IMEI] = new Array();
                    dataIDArray[packetInfo.IMEI].packet_id = 1;
                    dataIDArray[packetInfo.IMEI].sql_id = 0;
                }
                sendFirstQueuedCommand(packetInfo.IMEI);
                break;
            case 0x0001:
                // Device Information Report
                Utils.log('Got Device Information Packet');
                packetInfo.PROTOCOL_VERSION = tmp[1];
                packetInfo.ESN = tmp[5];
                packetInfo.PRODUCT_ID = tmp[6];
                packetInfo.SOFTWARE_VERSION = tmp[7];
                packetInfo.SUPPORT_PROTOCOLS = tmp[8];
                packetInfo.SEND_TIME = tmp[13];
                updateGarminInfo(packetInfo);
                break;
            case 0x0823:// Set Driver Status Receipt Report
                Utils.log('Got Driver Status Update');
                packetInfo.STATUS_ID = tmp[5];
                packetInfo.CHANGED_TIME = tmp[6];
                packetInfo.SEND_TIME = tmp[17];
                updateStatus(packetInfo);
                break;
            case 0x0813:// A607 Driver ID Update
                Utils.log('Got A607 Driver ID Update');
                packetInfo.DRIVER_ID = tmp[5];
                packetInfo.DRIVER_ID_CHANGED_TIME = tmp[6];
                packetInfo.SEND_TIME = tmp[17];
                updateDriverID(packetInfo);
                break;
            case 0x0802:// Set Driver Status List Text Receipt Report
                Utils.log('Got Driver Status List Receipt');
                packetInfo.STATUS_ID = tmp[5];
                packetInfo.RESULT = tmp[6];
                Utils.log((packetInfo.RESULT === '1') ? 'SUCCESS' : 'FAIL');
                break;
            case 0x0261:// Communication Link Status Report
                Utils.log('Got Communication Link Report');
                packetInfo.LINK_STATUS = tmp[6];
                packetInfo.SEND_TIME = tmp[17];
                Utils.log('Link Status: ' + (packetInfo.LINK_STATUS === '1' ? 'Established' : 'Lost'));
                updateLinkStatatus(packetInfo);
                sendFirstQueuedCommand(packetInfo.IMEI);
                break;
            case 0x0020:// Text Message ACK Report
                // TODO write Text Message ACK Report hander here
                Utils.log('Got Text Message ACK Report');
                break;
            case 0x0026:// A607 Client to Server Text Message
                // TODO write A607 Client to Server Text Message handler here
                packetInfo.MESSAGE_ID = tmp[5];
                packetInfo.LONGITUDE = tmp[8];
                packetInfo.LATITUDE = tmp[9];
                packetInfo.MESSAGE = tmp[6];
                packetInfo.REPLY_ID = tmp[7];
                packetInfo.SEND_TIME = tmp[19];

                Utils.log('Got A607 Client to Server Text Message');
                storeReceivedMessage(packetInfo);
                break;
            case 0x002B:// A604 Text Message Receipt Report
                // TODO write A604 Text Message Receipt Report  handler here
                Utils.log('Got A604 Text Message Receipt Report');
                packetInfo.MESSAGE_ID = tmp[5];
                packetInfo.WRITE_STATUS = tmp[6];
                packetInfo.SEND_TIME = tmp[17];
                updateSentMessageStatus(packetInfo);
                break;
            case 0x0041:// Text Message Status Report 
                // TODO write Text Message Status Report handler here
                Utils.log('Got Text Message Status Report ');
                break;
            case 0x0211:// Stop Status Report
                Utils.log('Got Stop Status Report');
                packetInfo.STOP_ID = tmp[5];
                packetInfo.STOP_STATUS = tmp[6];
                packetInfo.SEND_TIME = tmp[17];
                updateStopStatus(packetInfo);
                break;
//TODO create the handling function of below Garmin Packet IDs
            case 0x0029:// Canned Response list receipt report
            case 0x002B:// A604 Text Message Receipt Report
            case 0x0032:// Set Canned Response Receipt Report
            case 0x0033:// Delete Canned Response Receipt Reeport
            case 0x0034:// Refresh Canned Response List Text Report
            case 0x0051:// Set Cannned Message Receipt Report
            case 0x0053:// Delete Canned Message Receipt Report
            case 0x0054:// Refresh Canned Message List Report
            case 0x0111:// Sort Stop List Acknowledgement Report

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
    } else {
        console.error("Error: Unknown packet data type: " + tmp[0]);
    }

    if (callback) {
        callback(packetInfo);
    } else {
        return packetInfo;
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
    var client = new pg.Client(ConnectionInfo);
    client.connect(function(error) {
        if (error) {
            client.end();
            if (callback) {
                return callback(error);
            }
        }
        client.query(sql, function(error, result) {
            client.end();
            if (callback) {
                return callback(error, result);
            }
        });
    });
}

/**
 * Store Fixed Time Report Data
 * @param {type} packetInfo
 * @returns {Object} Error
 */
function updateBoxInfo(packetInfo) {
    !packetInfo.CHANGED_TIME ? packetInfo.CHANGED_TIME = '00000000000000' : '';
    !packetInfo.STATUS_ID ? packetInfo.STATUS_ID = -1 : '';
    pg_connectAndQuery("SELECT * FROM boxes WHERE imei='" + packetInfo.IMEI + "' LIMIT 1;", function(error, r) {
        if (error) {
            return error;
        }
        var sql;
        if (r.rows.length === 0) {
            //sql = "INSERT INTO boxes(imei, driver_id, status_id, last_update, last_change) VALUES ('" + packetInfo.IMEI + "', '" + packetInfo.DRIVER_ID + "', " + packetInfo.STATUS_ID + ", to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour',to_timestamp('" + packetInfo.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour' );";
            sql = "INSERT INTO boxes \n\
                (imei, driver_id, driver_id_changed, driver_status, driver_status_changed, last_update) \n\
                VALUES ('" + packetInfo.IMEI + "', '" + packetInfo.DRIVER_ID + "',\n\
 to_timestamp('" + packetInfo.DRIVER_ID_CHANGED_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour'," + packetInfo.STATUS_ID + ",\n\
 to_timestamp('" + packetInfo.STATUS_ID_CHANGED_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour',\n\
 to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour');";
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
            sql = "UPDATE 	boxes \n\
                SET 	driver_id = '" + packetInfo.DRIVER_ID + "', \n\
                driver_id_changed = to_timestamp('" + packetInfo.DRIVER_ID_CHANGED_TIME + "', 'YYYYMMDDHH24MISS') + interval '7 hour', \n\
                driver_status = " + packetInfo.STATUS_ID + ", \n\
                driver_status_changed = to_timestamp('" + packetInfo.STATUS_ID_CHANGED_TIME + "', 'YYYYMMDDHH24MISS') + interval '7 hour', \n\
                last_update = to_timestamp('" + packetInfo.SEND_TIME + "', 'YYYYMMDDHH24MISS') + interval '7 hour' \n\
                WHERE 	imei = '" + packetInfo.IMEI + "'; ";
            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Driver Info Update: Failed');
                    console.error(error);
                    return error;
                } else {
                    Utils.log('Driver Info Update: Success');
                }
            });
            if (!r.rows[0].protocol_version) {
                //queueUpCommand(packetInfo.IMEI, "AT+GTFMI=gv300,2,,,,,{packetID}\$");
                var command = "%CMDFWD%|" + packetInfo.IMEI + "|ABC1234|" + timeStamp() + "|'AT+GTFMI=gv300,2,,,,,FFFF$'";
                var message = new Buffer(command);
                var sendSock = datagram.createSocket('udp4');
                sendSock.send(message, 0, message.length, GatewayInfo.port, GatewayInfo.address, function(err, bytes) {
                    sendSock.close();
                });


            }


        }
    });
}


function updateDriverID(packetInfo) {

    var sql = "UPDATE boxes \n\
                SET driver_id = '" + packetInfo.DRIVER_ID + "',\n\
                driver_id_changed = to_timestamp('" + packetInfo.DRIVER_ID_CHANGED_TIME + "', 'YYYYMMDDHH24MISS') + interval '7 hour',\n\
                last_update = to_timestamp('" + packetInfo.SEND_TIME + "', 'YYYYMMDDHH24MISS') + interval '7 hour'\n\
                WHERE imei='" + packetInfo.IMEI + "';";
    pg_connectAndQuery(sql, function(error, result) {
        // nothing to do :D
    });
}


/**
 * Store status update data
 * @param {type} packetInfo
 * @returns {undefined}
 */
function updateStatus(packetInfo) {
    pg_connectAndQuery("SELECT * FROM boxes WHERE imei='" + packetInfo.IMEI + "' LIMIT 1;", function(error, r) {
        if (error) {
            return;
        }
        var sql;
        if (r.rows.length === 0) {
            //sql = "INSERT INTO boxes(imei, driver_id, status_id, last_update, last_change) VALUES ('" + packetInfo.IMEI + "', '" + packetInfo.DRIVER_ID + "', " + packetInfo.STATUS_ID + ", to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour' ,to_timestamp('" + packetInfo.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour' );";
            sql = "INSERT INTO boxes(\n\
                    imei, driver_id, driver_id_changed, driver_status, driver_status_changed,\n\
                    last_update)    \n\
                    VALUES ('" + packetInfo.IMEI + "', '" + packetInfo.DRIVER_ID + "',\n\
                    to_timestamp('00000000000000','YYYYMMDDHH24MISS')+ interval '7 hour',\n\
                    " + packetInfo.STATUS_ID + ",\n\
                    to_timestamp('" + packetInfo.CHANGED_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour',\n\
                    to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour');";
            pg_connectAndQuery(sql, function(error, rs) {
                if (error) {
                    Utils.log('Insert Box Detail: Failed');
                    console.error(error);
                } else {
                    Utils.log('Insert Box Detail: Success');
                }
            });
        } else {
            sql = "UPDATE boxes SET  driver_status=" + packetInfo.STATUS_ID + ", last_update=to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour'  , driver_id_changed=to_timestamp('" + packetInfo.CHANGED_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour'  WHERE imei='" + packetInfo.IMEI + "';";
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

function updateStopStatus(packetInfo) {
    var sql = "SELECT status FROM stop_points WHERE point_id=" + packetInfo.STOP_ID + " AND imei='" + packetInfo.IMEI + "' LIMIT 1;";
    console.log(sql);
    pg_connectAndQuery(sql, function(error, result) {
        if (!result.rows[0]) {
            sql = "INSERT INTO stop_points(point_id, imei, lat, lon, description, status, last_update, last_status) VALUES (  " + packetInfo.STOP_ID + ", " + packetInfo.IMEI + ", -1, -1, 'GARMIN LIST', " + packetInfo.STOP_STATUS + ", to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS') + interval '7 hour',-1);";
            console.log(sql);
        } else if (result.rows[0].status === '104') {
            sql = "UPDATE stop_points SET status=" + packetInfo.STOP_STATUS + ", last_update=to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour' WHERE point_id=" + packetInfo.STOP_ID + " AND imei='" + packetInfo.IMEI + "';";
        } else {
            sql = "UPDATE stop_points SET last_status=status, status=" + packetInfo.STOP_STATUS + ", last_update=to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour' WHERE point_id=" + packetInfo.STOP_ID + " AND imei='" + packetInfo.IMEI + "';";
        }
        pg_connectAndQuery(sql, function(error, result) {
            console.error(error);
        });
    });
}

function updateLinkStatatus(packetInfo) {
    var status = packetInfo.LINK_STATUS === '1' ? 'TRUE' : 'FALSE';
    var sql_update = "UPDATE boxes SET link_status = '" + status + "', last_update=to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour'\n\
         WHERE imei= '" + packetInfo.IMEI + "' ;";
    pg_connectAndQuery(sql_update, function(error, result) {
    });

}


function updateGarminInfo(packetInfo) {
//    packetInfo.PROTOCOL_VERSION = tmp[1];
//    packetInfo.ESN = tmp[5];
//    packetInfo.PRODUCT_ID[6];
//    packetInfo.SOFTWARE_VERSION = tmp[7];
//    packetInfo.SUPPORT_PROTOCOLS = tmp[8];
//    packetInfo.SEND_TIME = tmp[13];

    var sql = "UPDATE boxes SET protocol_version='" + packetInfo.PROTOCOL_VERSION + "',\n\
    esn='" + packetInfo.ESN + "',\n\
    product_id = '" + packetInfo.PRODUCT_ID + "',\n\
    software_version='" + packetInfo.SOFTWARE_VERSION + "',\n\
    support_protocols='" + packetInfo.SUPPORT_PROTOCOLS + "'\n\
    WHERE imei = '" + packetInfo.IMEI + "';";
    pg_connectAndQuery(sql, function(error, result) {
    });


}

function increaseRecvData(bytes) {
    var packetSize = bytes.length;
    var mesg = bytes.toString().split(',');
    var imei = mesg[2];
    var sql_insert = "INSERT INTO data_stats(imei, received, last_update, start_date)\n\
                        VALUES ('" + imei + "', " + packetSize + ", now(), now());";
    var sql_update = "UPDATE data_stats SET received=received+" + packetSize + ",last_update=now() WHERE imei='" + imei + "'";

    pg_connectAndQuery(sql_insert, function(error, result) {
        if (error) {
            pg_connectAndQuery(sql_update, function(error, result) {
            });
        }
    });

}
function increaseSentData(imei, bytes) {
    var packetSize = bytes.length;
    var sql_insert = "INSERT INTO data_stats(imei, sent, last_update, start_date)\n\
                        VALUES ('" + imei + "', " + packetSize + ", now(), now());";

    var sql_update = "UPDATE data_stats SET sent=sent+" + packetSize + ",last_update=now() WHERE imei='" + imei + "'";
    pg_connectAndQuery(sql_insert, function(error, result) {
        if (error) {
            pg_connectAndQuery(sql_update, function(error, result) {
            });
        }
    });

}


function queueUpCommand(imei, cmd) {
    var command = "%CMDFWD%|" + imei + "|ABC1234|" + timeStamp() + "|'" + cmd + "'";
    var sql = "INSERT INTO command_queue (addtime,imei,command) VALUES (now(),$1,$2);";
    var client = new pg.Client(ConnectionInfo);
    client.connect(function(error) {
        if (error) {
            client.end();
            return  error;
        }
        client.query(sql, [imei, command], function(error, result) {
            client.end();
        });

    });

}

function sendFirstQueuedCommand(imei) {
    if (!imei)
        return;
    var sql = "SELECT *  FROM command_queue WHERE imei='" + imei + "'  ORDER BY addtime ASC LIMIT 1;";
    pg_connectAndQuery(sql, function(error, result) {
        if (error) {
            console.error(error);
            return error;
        }
        if (!result.rows[0])
            return;
        var row = result.rows[0].command;
        dataIDArray[imei].sql_id = result.rows[0].command_id;
        if (dataIDArray[imei].packet_id >= 65535) {
            dataIDArray[imei].packet_id = 1;
        }
        var hexNum = padding((dataIDArray[imei].packet_id).toString(16).toUpperCase());
        var message = new Buffer(row.replace('{packetID}', hexNum));
        Utils.log('Sending Queued Command: ' + message.toString());
        var stringMessage = message.toString();

        var msg = stringMessage.substring(stringMessage.indexOf("'") + 1, stringMessage.lastIndexOf("'"));
        var realBytes = Buffer(msg);
        increaseSentData(imei, realBytes);
        var sendSock = datagram.createSocket('udp4');
        sendSock.send(message, 0, message.length, GatewayInfo.port, GatewayInfo.address, function(err, bytes) {
            console.log("error: " + err + " | bytes: " + bytes);
            sendSock.close();
        });
    });
}

function padding(hex) {
    while (hex.length < 4) {
        hex = '0' + hex;
    }
    return hex;
}

function deleteSentQueuedCommand(command_id) {
    var sql = "DELETE FROM command_queue WHERE command_id = " + command_id + ";";
    pg_connectAndQuery(sql, function(error, result) {
    });
}



//TODO continue on updateSentMessage Later
function updateSentMessageStatus(packetInfo) {
    // TODO cont. here

}

// TODO store the receive message
function storeReceivedMessage(packetInfo) {
    if(packetInfo.REPLY_ID==='')
        packetInfo.REPLY_ID = 'independent';
    var insert = "INSERT INTO recv_messages (imei,lat,lon,reply_id,message,last_update,message_id) \n\
    VALUES ('" + packetInfo.IMEI + "'," + packetInfo.LATITUDE + " ," + packetInfo.LONGITUDE + ",'" + packetInfo.REPLY_ID + "','" + packetInfo.MESSAGE + "', to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour', '"+packetInfo.MESSAGE_ID+" ' );";
    console.log(insert);
    pg_connectAndQuery(insert, function(error, result) {
        if(error){
            console.error(error);
        }
    });
}

/**
 * Timestamp in 'Y-m-d H:i:s' format
 * @returns {String}
 */
function timeStamp() {
    //$time = date('Y-m-d H:i:s');
    var d = new Date();
    var timeString = "";
    timeString += d.getFullYear();
    timeString += '-' + (d.getMonth() > 10 ? d.getMonth() : '0' + d.getMonth());
    timeString += '-' + (d.getDate() > 10 ? d.getDate() : '0' + d.getDate());
    timeString += ' ' + (d.getHours() > 10 ? d.getHours() : '0' + d.getHours());
    timeString += ":" + (d.getMinutes() > 10 ? d.getMinutes() : '0' + d.getMinutes());
    timeString += ":" + (d.getSeconds() > 10 ? d.getSeconds() : '0' + d.getSeconds());
    return timeString;
}
