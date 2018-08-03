const EventEmitter = require('events');

const Cursor = require('pg-cursor');

const statuses = {
    CONNECTING: 'connecting',
    CONNECTED: 'connected',
    DISCONNECTED: 'disconnected'
};


function wait(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}


function getNotificationClient(channel, connection, logEmitter) {
    const internalEmitter = new EventEmitter();
    let status = statuses.DISCONNECTED;
    let client;
    let reconnectsCount = 0;
    let isStopped = true;

    const reconnectionTimeout = connection.getReconnectionTimeout();

    function onDisconnect() {
        status = statuses.DISCONNECTED;
        logEmitter.emit('warn', 'Connection closed.');
        internalEmitter.emit('disconnect');

        return connect();
    }

    async function connect() {
        if (isStopped || status !== statuses.DISCONNECTED) {
            return;
        }

        status = statuses.CONNECTING;

        logEmitter.emit('info', 'Connecting...');
        client = connection.getClient();

        try {
            await client.connect();

            client.on('notification', message => internalEmitter.emit('notification', message));
            client.on('error', err => logEmitter.emit('warn', err.message));
            client.on('end', () => { onDisconnect(); });

            await client.query(`LISTEN "${channel}"`);

            status = statuses.CONNECTED;
            logEmitter.emit('info', 'Connected!');
            internalEmitter.emit('connect');
        } catch (err) {
            status = statuses.DISCONNECTED;
            logEmitter.emit('warn', `Connection failed, reconnect in ${Math.ceil(reconnectionTimeout / 1000)}s.`);
            await wait(reconnectionTimeout);
            reconnectsCount++;
            await connect();
        }
    }

    function start() {
        if (isStopped) {
            isStopped = false;
            connect();
        }
    }
    
    function end() {
        isStopped = true;
        if (status === statuses.CONNECTED) {
            client.end();
        }
    }

    function on(eventName, listener) {
        internalEmitter.on(eventName, listener);
    }

    function getStatus() {
        return {
            connection: status,
            reconnects: reconnectsCount
        }
    }

    return {
        start,
        end,
        on,
        getStatus
    };
}

function getQueryClient(table, connection, logEmitter) {
    const pool = connection.getPool();
    pool.on('error', (err) => { logEmitter.emit('warn', err.message); });

    async function getLastId() {
        const client = await pool.connect();

        const res = await client.query(`SELECT id FROM "${table}" ORDER BY id DESC LIMIT 1`);
        client.release();

        if (res.rows.length) {
            return Number(res.rows[0].id);
        }

        return 0;
    }

    async function getRecordsIterator(minId, maxId, batchSize) {
        let sql = `SELECT * FROM "${table}"`;
        let values = [minId];

        if (minId === maxId) {
            sql += ' WHERE id = $1';
        } else {
            sql += ' WHERE id >= $1';

            if (maxId) {
                sql += ' AND id <= $2';
                values.push(maxId);
            }
        }

        const client = await pool.connect();
        const cursor = client.query(new Cursor(sql, values));

        let hasMore = true;
        client.on('error', (err) => {
            logEmitter.emit('warn', err.message);
            hasMore = false;
        });

        async function* iterator() {
            while (hasMore) {
                let records;
                try {
                    records = await new Promise((resolve, reject) => {
                        cursor.read(batchSize, (err, rows) => err ? reject(err) : resolve(rows));
                    });
                } catch (err) {
                    logEmitter.emit('warn', err.message);
                    return undefined;
                }

                if (records.length < batchSize) {
                    hasMore = false;
                    cursor.close(() => {
                        client.release();
                    });
                }

                for (let record of records) {
                    yield { event: record.event, id: Number(record.id) };
                }
            }
        }

        return iterator;
    }

    return {
        getLastId,
        getRecordsIterator
    };
}

module.exports = { getNotificationClient, getQueryClient };
