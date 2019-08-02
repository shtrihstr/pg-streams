const { Pool } = require('pg');

const { TABLE_NAME, } = require('./const');
const {
    streamPredicate,
    stringPredicate,
    positiveIntPredicate,
    objectPredicate,
    optionalObjectPredicate
} = require('./predicates');

function createProducer({ pgOptions }) {
    const pool = new Pool(pgOptions);

    async function write({ stream, type, schemaVersion, data, meta }) {
        streamPredicate(stream);
        stringPredicate(type);
        positiveIntPredicate(schemaVersion);
        objectPredicate(data);
        optionalObjectPredicate(meta);

        const query = {
            text: `INSERT INTO "${TABLE_NAME}" ("stream", "type", "schemaVersion", "data", "meta") VALUES ($1, $2, $3, $4, $5)`,
            values: [stream, type, schemaVersion, data, (meta || {})],
        };

        await pool.query(query);
    }

    return { write };
}

module.exports = createProducer;
