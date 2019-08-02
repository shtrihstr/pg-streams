const EventEmitter = require('events');

const { Client } = require('pg');

const { NOTIFICATION_CHANNEL } = require('./const');

class SubscriptionEmitter extends EventEmitter {}


function createConsumerSubscription({ pgOptions, streams, reconnectDelay = 1000 }) {
    const emitter = new SubscriptionEmitter();
    let client;
    let isReconnecting = false;
    let isStopped = true;


    async function connect() {
        if (client) {
            throw new Error('Consumer subscription connection already exist');
        }

        client = new Client({ ...pgOptions, keepAlive: true });

        client.on('notification', onNotification);
        client.on('error', (err) => {
            emitter.emit('warn', err.message);
            reconnect().catch(err => emitter.emit('error', err));
        });
        client.on('end', () => {
            if (!isStopped) {
                reconnect().catch(err => emitter.emit('error', err));
            }
        });

        await client.connect();
        client.query(`LISTEN "${NOTIFICATION_CHANNEL}"`).catch(((err) => {
            emitter.emit('warn', err.message);
            reconnect().catch(err => emitter.emit('error', err));
        }));

        emitter.emit('connected');
    }

    async function reconnect(force = false) {
        if (isReconnecting && !force) {
            return;
        }

        emitter.emit('info', 'Reconnecting...');

        isReconnecting = true;

        if (client) {
            client.end().catch(() => {});
            client = null;
        }

        try {
            await connect();
        }
        catch (err) {
            emitter.emit('warn',`Reconnection error: ${err.message}`);
            emitter.emit('info', `Reconnecting in: ${reconnectDelay}`);
            setTimeout(() => reconnect(true).catch(err => emitter.emit('error', err)), reconnectDelay);
            return;
        }

        isReconnecting = false;
    }

    function onNotification(message) {
        if (!message.payload || !message.payload.startsWith('NEW_EVENT')) {
            return;
        }

        const [, position, stream] = message.payload.split(' ');

        if (!streams.includes(stream)) {
            return;
        }

        emitter.emit('notification', { stream, position: BigInt(position) });

    }

    return {
        async start() {
            if (!isStopped) {
                throw new Error('Consumer subscription already started');
            }
            isStopped = false;
            await connect();
        },

        async stop() {
            isStopped = true;
            try {
                await client.end();
            } catch {}
            client = null;
        },

        on(eventName, listener) {
            emitter.on(eventName, listener);
        }
    }
}


module.exports = createConsumerSubscription;
