const { Pool, Client } = require('pg');

function createConnection(options) {
    const mergedOptions = Object.assign({
        connectionTimeoutMillis: 3000,
        max: 3
    }, options, { statement_timeout: 0 });

    const pool = new Pool(mergedOptions);

    function getPool() {
        return pool;
    }

    function getClient() {
        return new Client(mergedOptions);
    }

    function getReconnectionTimeout() {
        return mergedOptions.connectionTimeoutMillis;
    }

    return {
        getPool,
        getClient,
        getReconnectionTimeout
    };
}

module.exports = { createConnection };
