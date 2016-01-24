'use strict'; /*jslint node: true, es5: true, indent: 2 */
var Promise = require('bluebird');
var options = {
    promiseLib: Promise
};
var pgp = require('pg-promise')(options);

var path = require('path');
var moment = require('moment');
global.appName = path.basename(__dirname);
var totalrecords = 0;

var Logger = require('node-bunyan-gcalendar');
var calendar_level = 'fatal';

var bunyanoptions = {
    name: process.env.HEROKU_APP_NAME + ':' + appName,
    streams: [{
        level: 'debug',
        stream: process.stdout
    }, {
        level: 'info',
        path: path.resolve(process.env.NJSAGENT_APPROOT + '/logs/' + appName + '.log')
    }]
};

var log;

new Logger(bunyanoptions, process.env.NBGC_AES, process.env.NBGC_KEY, calendar_level).then(function logOk(l) {

    log = l;
    log.info('Logging started');

    if (!process.env.DXCOLLECTOR_PGSQL) {
        log.error('Missing DXCOLLECTOR_PGSQL environment variable');
        process.exit(1);
    } else {
        log.trace('DXCOLLECTOR_PGSQL=' + process.env.TWCOLLECTOR_PGSQL);
    }

    //
    // Handle signals
    //
    var stopSignals = [
        'SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT',
        'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM'
    ];
    stopSignals.forEach(function(signal) {
        process.once(signal, function(s) {
            log.info('Got signal, exiting...');
            setTimeout(function() {
                process.exit(1);
            }, 1000);
        });
    });
    process.once('uncaughtException', function(err) {
        log.fatal('Uncaught Exception.');
        log.error(err);
        setTimeout(function() {
            process.exit(1);
        }, 1000);
    });

    var db = pgp(process.env.DXCOLLECTOR_PGSQL);

    //
    // Connect to telnet server
    //
    var configdata = require('./dxproxy.conf.json');
    var options = {
        idleTimeout: 0, // Sets the socket to timeout after timeout milliseconds of inactivity on the socket. If timeout is 0, then the existing idle timeout is disabled.  
        reconnectWait: 1000, // Time (ms) before attempting to reconnect
        maxReconnects: 2, // Number of reconnects to try before moving to next server
        maxCycles: 2, // Times to try all servers before fail event emitted 
        servers: configdata.servers
    };

    var dxstream = require('./lib/serverpoll');
    var dxdata = dxstream.connect(log, options);

    dxdata.on('connect', function(connection) {
        // ...connected, event may occur many times.
        log.info('Connected to DX Cluster at ' + connection.info.host + ':' + connection.info.port);
        connection.socket.write(connection.info.login);
    });

    dxdata.on('hosterror', function(err) {
        // ...could not connect to a specific server.
        log.warn('Wrong host name ' + err.host + ':' + err.port);
    });

    dxdata.on('hostfailed', function(err) {
        // ...could not connect to a specific server.
        log.warn('Failed to connect to ' + err.host + ':' + err.port);
    });

    dxdata.on('fail', function() {
        // ...could not connect to any server.
        log.error('Could not connect to any server.');
        client.end();
        setTimeout(function() {
            process.exit(1);
        }, 2000);
    });

    dxdata.on('data', function(data) {
        var cluster_data, line, now, utc_hour, utc_minute;
        now = new Date();
        cluster_data = {};
        line = data.toString();
        if (line.match(/^DX/)) {
            cluster_data.call = /^[a-z0-9\/]*/i.exec(line.substring(6, 16))[0];
            cluster_data.freq = line.substring(16, 24).replace( /^\D+/g, '').trim();
            cluster_data.dxcall = /^[a-z0-9\/]*/i.exec(line.substring(26, 38))[0];
            cluster_data.comment = line.substring(39, 69).trim();
            utc_hour = line.substring(70, 72);
            utc_minute = line.substring(72, 74);
            cluster_data.utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), utc_hour, utc_minute);
            cluster_data.ts = moment().utc().toISOString();
            log.trace("dxdata: " + JSON.stringify(cluster_data));
            totalrecords = totalrecords + 1;
            var idata = {data: [cluster_data]};
            var sql = 'INSERT INTO public.dxdata (dxdata_date, dxdata_jdata) VALUES (DEFAULT, $1) ON CONFLICT (dxdata_date) DO UPDATE set dxdata_jdata=jsonb_set(dxdata.dxdata_jdata::jsonb, \'{data,2147483647}\', $2, true)';
            db.none(sql, [idata, cluster_data]).then(function success(data) {                
                log.trace("Wrote", JSON.stringify(cluster_data));
            }, function failure(err) {
                log.warn("Failed to insert ", err, sql);
            });
        }
    });

    setInterval(function() {
        log.info('Got ' + totalrecords + ' records');
    }, 1000 * 60 * 10);


}, function logNotOK(err) {
    console.error('Logging start failed: ', err);
});