const { getTableName } = require('../helpers');

function createProducer(streamName, connection) {
    const tableName = getTableName(streamName);
    const pool = connection.getPool();
    
    async function send(event) {
        if (typeof event !== 'string') {
            throw new Error('Event must be a string.');
        }

        const client = await pool.connect();
        const res = await client.query(`INSERT INTO "${tableName}" (event) VALUES ($1) RETURNING id`, [event]);
        client.release();
        return Number(res.rows[0].id);
    }

    return {
        send
    };
}

module.exports = { createProducer };
