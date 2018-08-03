# Postgres Streams

*NOTE: The current library is in an early development stage. DO NOT USE IT!*

## Idea
`<...>`

## Usage
### Install
`$ npm i pg pg-cursor pg-streams`

### Create stream
```js
const { createConnection, createStreamManager } = require('pg-streams');

const pgOptions = {
    user: 'postgres',
    host: 'localhost',
    database: 'streams'
};
const connection = createConnection(pgOptions);
const streamManager = createStreamManager(connection);

streamManager.createStream('stream-name');
```

### Producer
```js
const { createConnection, createProducer } = require('pg-streams');

const connection = createConnection(pgOptions);
const producer = createProducer('stream-name', connection);

const event = {
    foo: 'bar'
};

producer.send(JSON.stringify(event));
```

### Consumer
```js
const { createConnection, createConsumer } = require('pg-streams');

const connection = createConnection(pgOptions);
const consumer = createConsumer('stream-name', connection, consumeHandler);

consumer.on('error', console.error);
consumer.on('info', console.log);
consumer.on('warn', console.log);

consumer.startFrom(0);

async function consumeHandler(event, id) {
    await doCoolStuff(event);
}
```