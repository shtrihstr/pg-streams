const EventEmitter = require('events');

const ow = require('ow');

const { getTableName, getChannelName } = require('../helpers');

const { getNotificationClient, getQueryClient } = require('./clients');

const BATCH_SIZE = 10;

function createConsumer(streamName, connection, handler) {
    ow(streamName, ow.string.label('streamName').nonEmpty);
    ow(connection, ow.object.label('connection').hasKeys('getPool', 'getClient', 'getReconnectionTimeout'));
    ow(handler, ow.function.label('handler'));

    const tableName = getTableName(streamName);
    const channel = getChannelName(streamName);

    const logEmitter = new EventEmitter();
    let isStopped = true;
    let notificationClient;

    async function start(offset) {
        notificationClient = getNotificationClient(channel, connection, logEmitter);
        const queryClient = getQueryClient(tableName, connection, logEmitter);

        let isConsuming = false;
        let isHasMoreToConsume = true;
        let lastAddedEventId = 0;
        let lastConsumedId = offset - 1;

        async function consume(minId, maxId) {
            if (isStopped || maxId < minId) {
                return;
            }

            isConsuming = true;
            isHasMoreToConsume = false;

            logEmitter.emit('info', `SELECT records from ${minId} to ${maxId}.`);
            let iterator;
            try {
                iterator = await queryClient.getRecordsIterator(minId, maxId, BATCH_SIZE);
            } catch (err) {
                isConsuming = false;
                return;
            }

            for await (let row of iterator()) {
                lastConsumedId = row.id;
                try {
                    await handler(row.event, row.id);
                } catch (err) {
                    stop();
                    throw err;
                }

                if (isStopped) {
                    return;
                }
            }

            if (isHasMoreToConsume) {
                return consume(lastConsumedId + 1, lastAddedEventId);
            }

            isConsuming = false;
        }

        notificationClient.on('notification', (message) => {
            console.log(message);
            if (message.channel === channel) {
                const id = Number(message.payload);
                if (id > lastAddedEventId) {
                    logEmitter.emit('info', `Receive INSERT notification. Record ID: ${id}.`);
                    isHasMoreToConsume = true;
                    lastAddedEventId = id;
                    if (!isConsuming) {
                        consume(lastConsumedId + 1, lastAddedEventId);
                    }
                }
            }
        });

        notificationClient.on('connect', async () => {
            try {
                lastAddedEventId = await queryClient.getLastId();
            } catch (err) {
                logEmitter.emit('warn', err.message);
            }

            if (isConsuming) {
                isHasMoreToConsume = true;
            } else {
                consume(lastConsumedId + 1, lastAddedEventId);
            }
        });

        notificationClient.start();

    }

    function stop() {
        isStopped = true;
        if (notificationClient) {
            notificationClient.end();
        }
    }

    function on(eventName, listener) {
        logEmitter.on(eventName, listener);
    }

    function startFrom(offset) {
        ow(offset, ow.number.label('offset').integer.greaterThanOrEqual(0));
        if (!isStopped) {
            throw new Error('Consumer is already started.');
        }

        isStopped = false;

        start(offset).catch((err) => {
            logEmitter.emit('error', err);
        });
    }

    function getStatus() {
        let notificationClientStatus;
        if (notificationClient) {
            notificationClientStatus = notificationClient.getStatus();
        } else {
            notificationClientStatus = { connection: 'disconnected' };
        }

        return {
            notification: notificationClientStatus
        }
    }

    return {
        startFrom,
        on,
        getStatus
    };

}

module.exports = { createConsumer };
