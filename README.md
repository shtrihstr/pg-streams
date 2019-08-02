# Postgres Streams

*NOTE: The current library is in an early development stage. DO NOT USE IT!*

## Idea
`<...>`

## Usage
### 1. Setup PostgreSQL
```sql
CREATE TABLE IF NOT EXISTS pg_streams_events (
  "position" bigserial PRIMARY KEY,
  "stream" text NOT NULL,
  "type" text NOT NULL,
  "schemaVersion" serial NOT NULL,
  "data" jsonb NOT NULL,
  "meta" jsonb NOT NULL,
  "createdAt" timestamp NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pg_streams_events_stream ON pg_streams_events ("stream");

CREATE OR REPLACE FUNCTION pg_streams_notify() RETURNS trigger AS $trigger$
  DECLARE
  BEGIN
    PERFORM pg_notify('pg_streams', 'NEW_EVENT ' || NEW."position" || ' ' || NEW."stream");
    RETURN NEW;
  END;
$trigger$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS pg_streams_insert_trigger ON pg_streams_events;
CREATE TRIGGER pg_streams_insert_trigger AFTER INSERT ON pg_streams_events
FOR EACH ROW EXECUTE PROCEDURE pg_streams_notify();
```
### 2. Install NPM module
`$ npm i pg pg-streams`

### 3. Producer
```js
const { createProducer } = require('pg-streams');

const producer = createProducer({ pgOptions });

producer.write({
   stream: 'user-account',
   type: 'created',
   schemaVersion: 2,
   data: {
      name: "Bob",
      age: 33,
      sex: undefined
   },
   meta: {
      foo: 'bar'
   }
});
```

### 4. Consumer
```js
const { createConsumer } = require('pg-streams');

let position = '0';

const consumer = createConsumer({
    pgOptions,
    streams: ['user-account', 'user-preferences'],
    fromPosition: position,
    handler: async (event) => {
      console.log(event);
      position = event.position
   }
});

consumer.on('error', console.error);
consumer.on('warn', console.log);
consumer.on('info', console.log);

consumer.start();
```
