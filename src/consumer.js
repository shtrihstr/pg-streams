const EventEmitter = require('events');

const { Pool } = require('pg');

const { TABLE_NAME, BATCH_SIZE } = require('./const');
const { streamsPredicate, positionPredicate, asyncFunctionPredicate } = require('./predicates');
const createConsumerSubscription = require('./consumer-subscription');

class ConsumerEmitter extends EventEmitter {}

function createConsumer({ pgOptions, streams, fromPosition, handler }) {
    streamsPredicate(streams);
    positionPredicate(fromPosition);
    asyncFunctionPredicate(handler);

    const emitter = new ConsumerEmitter();

    let consumedPosition = BigInt(fromPosition) - 1n;
    let notifiedPosition = BigInt(0);

    let isConsuming = false;
    let isStarted = false;

    const pool = new Pool(pgOptions);

    pool.on('error', (error) => emitter.emit('error', error));

    const subscription = createConsumerSubscription({ pgOptions, streams });

    subscription.on('warn', warn => emitter.emit('warn', warn));
    subscription.on('info', warn => emitter.emit('info', warn));
    subscription.on('error', error => emitter.emit('error', error));

    subscription.on('notification', ({ stream, position }) => {
        notifiedPosition = position;
        emitter.emit('info', `New event was added to stream "${stream}"`);
        if (!isConsuming && isHasMoreToConsume()) {
            consume().catch(err => emitter.emit('error', err));
        }
    });

    subscription.on('connected', () => {
        consume().catch(error => emitter.emit('error', error));
    });

    async function getEvents(from, limit) {
        console.log('sELecT');
        const query = {
            text: `SELECT * FROM "${TABLE_NAME}" WHERE "position" >= $1 AND "stream" = ANY($2) ORDER BY "position" LIMIT $3`,
            values: [from, streams, limit]
        };

        try {
            const result = await pool.query(query);
            return result.rows;
        } catch (err) {
            return [];
        }
    }

    function isHasMoreToConsume() {
        return notifiedPosition > consumedPosition
    }

    async function consume() {
        if (isConsuming || !isStarted) {
            return undefined;
        }
        isConsuming = true;

        emitter.emit('info', `Consuming events from position ${consumedPosition + 1n}...`);

        // TODO: consider using cursor
        while (true) {
            const events = await getEvents(consumedPosition + 1n, BATCH_SIZE);

            for (const event of events) {
                try {
                    await handler(event)
                } catch (err) {
                    emitter.emit('error', err);
                    return stop();
                }

                consumedPosition = BigInt(event.position);
            }

            if (events.length < BATCH_SIZE) {
                isConsuming = false;

                if (isHasMoreToConsume()) {
                    return consume();
                }

                return undefined;
            }
        }
    }

    return {
        async start() {
            if (!isStarted) {
                isStarted = true;
                await subscription.start();
            }
        },

        async stop() {
            if (isStarted) {
                isStarted = false;
                isConsuming = false;
                await
                subscription.stop();
            }
        },

        on(eventName, listener) {
            emitter.on(eventName, listener);
        }
    };
}

module.exports = createConsumer;
