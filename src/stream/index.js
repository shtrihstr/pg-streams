const fs = require('fs');

const { getTableName } = require('../helpers');

const TABLE_REGEX = /{{table}}/g;

const createStreamTableSql = loadSQL('create-stream-table');
const createNotifyFunctionSql = loadSQL('create-notify-function');
const createTriggerSql = loadSQL('create-trigger');

function createStreamManager(connection) {
    const pool = connection.getPool();

    async function createStream(name) {
        const table = getTableName(name);
        const client = await pool.connect();

        const isExists = await isTableExists(client, table);
        if (isExists) {
            client.release();
            return false;
        }

        // check if notify function exists
        const res = await client.query('SELECT COUNT(proname) > 0 AS "isExists" FROM pg_proc WHERE proname = \'pg_streams_notify\'');
        if (!res.rows[0].isExists) {
            await client.query(createNotifyFunctionSql);
        }

        await client.query(createStreamTableSql.replace(TABLE_REGEX, table));
        await client.query(createTriggerSql.replace(TABLE_REGEX, table));
        client.release();

        return true;
    }

    async function isStreamExists(name) {
        const table = getTableName(name);
        const client = await pool.connect();
        const isExists = await isTableExists(client, table);
        client.release();
        return isExists;
    }

    return {
        createStream,
        isStreamExists
    };
}

function loadSQL(name) {
    const filename = `${__dirname}/sql/${name}.sql`;
    return fs.readFileSync(filename, 'utf-8');
}

async function isTableExists(client, table) {
    const res = await client.query(`SELECT COUNT(to_regclass('${table}')) > 0 AS "isExists"`);
    return res.rows[0].isExists;
}

module.exports = { createStreamManager };
