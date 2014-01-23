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
    // - DEV -  packet viewer - - - - - -
    for (var i = 0; i < tmp.length; i++) {
        console.log(i + ' : ' + tmp[i]);
    }
    // - - - - - - - - - - - - - - - - - -
    var packetInfo = new Array();
    packetInfo.HEX = parseInt(tmp[4], 16);
    packetInfo.IMEI = tmp[2];
    switch (packetInfo.HEX) {
        case 0x0000:
            // Greeting from Garmin
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
            packetInfo.DIST_TO_DEST = tmp[15];// m
            packetInfo.DEST_LONGITUDE = tmp[16];
            packetInfo.DEST_LATITUDE = tmp[17];
            packetInfo.DRIVER_INDEX = tmp[20];

            updateBoxInfo(packetInfo);
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
            Utils.log((packetInfo.RESULT === 1) ? 'SUCCESS' : 'FAIL');
            break;

        case 0x0261:// Communication Link Status Report
            Utils.log('Got Communication Link Report');
            packetInfo.LINK_STATUS = tmp[6];
            Utils.log('Link Status: ' + (packetInfo.LINK_STATUS === 1 ? 'Established' : 'Lost'));
            break;

        case 0x0020:// Text Message ACK Report
            // TODO write Text Message ACK Report hander here
            Utils.log('Got Text Message ACK Report');
            break;

        case 0x0026:// A607 Client to Server Text Message
            // TODO write A607 Client to Server Text Message handler here
            Utils.log('Got A607 Client to Server Text Message');
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
    var client = new pg.Client(connectionInfo);
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
    var sql = "UPDATE stop_points SET status=" + packetInfo.STOP_STATUS + ", last_update=to_timestamp(" + packetInfo.SEND_TIME + ") WHERE point_id=" + packetInfo.STOP_ID + " AND imei='" + packetInfo.IMEI + "';";
    pg_connectAndQuery(sql, function(error, result) {
        if (error) {
            sql = "INSERT INTO stop_points(point_id, imei, lat, lon, description, status, last_update) VALUES (  " + packetInfo.STOP_ID + ", " + packetInfo.IMEI + ", -1, -1, 'GARMIN LIST', " + packetInfo.STOP_STATUS + ", to_timestamp(" + packetInfo.SEND_TIME + "));";
            pg_connectAndQuery(sql, function(error, result) {
            });
        }
    });
}


//TODO continue on updateSentMessage Later
function updateSentMessageStatus(packetInfo) {
    // TODO cont. here
    var sql = "UPDATE sent_message SET message_write_status=" + packetInfo.WRITE_STATUS + ", last_update=to_timestamp('" + packetInfo.SEND_TIME + "','YYYYMMDDHH24MISS')+ interval '7 hour'  , WHERE imei='" + packetInfo.IMEI + "';";
//    pg_connectAndQuery(sql, function(error, result) {
//        
//    });

}