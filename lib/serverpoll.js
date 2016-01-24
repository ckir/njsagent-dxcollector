var net = require('net');
var events = require('events');
var log;

exports.connect = function(logger, opts) {

    log = logger;
    var ee = new events.EventEmitter();

    if (typeof opts === 'function') {
        opts = {};
    }

    opts = opts || {
        allowHalfOpen: true
    };

    opts.servers = opts.servers || [{
        port: 9099,
        host: '127.0.0.1'
    }];

//    Array.prototype.remove = function(from, to) {
//        var rest = this.slice((to || from) + 1 || this.length);
//        this.length = from < 0 ? this.length + from : from;
//        return this.push.apply(this, rest);
//    };

    for (var count = 0; count < opts.servers.length; count++) {
        if (!net.isIP(opts.servers[count].host)) {
            log.warn('Invalid host: ' + opts.servers[count].host + ' - removed from list');
            ee.emit('hosterror', new Error(opts.servers[count]));
            //opts.servers.remove(count);
            opts.servers.splice(count, 1);
        }
    }

    if (opts.servers.length === 0) {
        ee.emit('fail', 'No servers');
    }

    var serverIndex = 0;
    var reconnectTries = 0;
    var cyclesCompleted = 0;
    var client;

    var idleTimeout = opts.idleTimeout || 0; // Sets the socket to timeout after timeout milliseconds of inactivity on the socket. If timeout is 0, then the existing idle timeout is disabled.	
    var reconnectWait = opts.reconnectWait || 1000; // Time before attempting to reconnect
    var maxReconnects = opts.maxReconnects || 2; // Number of reconnects to try before moving to next server
    var maxCycles = opts.maxCycles || 1;

    function connect() {

        var service = opts.servers[serverIndex];

        var client = net.Socket(opts);

        client.on('connect', function() {
            ee.emit('connect', {
                socket: client,
                info: service
            });
        });

        client.on('data', function(data) {
            ee.emit('data', data);
        });

        client.on('reconnect', function() {
            reconnectTries++;
            if (reconnectTries > maxReconnects) {
                reconnectTries = 0;
                ee.emit('error', 'maxReconnects reached. Moving to next server');
            } else {
                log.info('Reconnected following a close or timeout event. Try ... ' + reconnectTries);
            }
        });

        client.on('error', function(e) {
            reconnectTries++;
            if (reconnectTries > maxReconnects) {
                reconnectTries = 0;
                log.warn("Failed reconnecting to " + service.host + ':' + service.port + ' after a ' + e.errno + ' event. maxReconnects reached. Moving to next server.');
                ee.emit('hostfailed', service);
                serverIndex++;
                if (serverIndex >= opts.servers.length) {
                    cyclesCompleted++;
                    if (cyclesCompleted >= maxCycles) {
                        ee.emit('fail', 'No server is available');
                    } else {
                        serverIndex = 0;
                        service = opts.servers[serverIndex];
                        log.info("Connecting to " + service.host + ':' + service.port);
                        client.connect(service.port, service.host);
                    }
                } else {
                    service = opts.servers[serverIndex];
                    log.info("Connecting to " + service.host + ':' + service.port);
                    client.connect(service.port, service.host);
                }
            } else {
                log.info('Waiting for ' + reconnectWait + '(ms) before reconnecting to ' + service.host + ':' + service.port);
                setTimeout(function() {
                    log.info("Reconnecting to " + service.host + ':' + service.port + ' after a ' + e.errno + ' event. Try no ... ' + reconnectTries);
                    client.setTimeout(idleTimeout);
                    client.connect(service.port, service.host);
                }, reconnectWait);
            }
        });

        client.setEncoding("utf8");
        client.setTimeout(idleTimeout);
        // .setKeepAlive([enable], [initialDelay])
        log.info("Connecting to " + service.host + ':' + service.port);
        client.connect(service.port, service.host);
    }

    connect();

    return ee;
};
