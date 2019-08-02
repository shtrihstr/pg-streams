const ow = require('ow');

const streamPredicate = ow.create(ow.string.nonEmpty.matches(/^[a-zA-z0-9\-_]+$/));
const streamsPredicate = ow.create(ow.array.nonEmpty.ofType(ow.string.nonEmpty.matches(/^[a-zA-z0-9\-_]+$/)));
const stringPredicate = ow.create(ow.string.nonEmpty);
const positiveIntPredicate = ow.create(ow.number.integer.positive);
const positionPredicate = ow.create(ow.string.nonEmpty.numeric);
const objectPredicate = ow.create(ow.object.nonEmpty);
const asyncFunctionPredicate = ow.create(ow.function.is(fn => fn.constructor.name === 'AsyncFunction' || 'Expected async function'));
const optionalObjectPredicate = ow.create(ow.optional.object);

module.exports = {
    streamPredicate,
    streamsPredicate,
    stringPredicate,
    positiveIntPredicate,
    positionPredicate,
    objectPredicate,
    asyncFunctionPredicate,
    optionalObjectPredicate
};
