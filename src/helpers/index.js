
function satinize(string) {
    return string.replace(/[- .:]+/, '_').replace(/[^a-zA-Z0-9_]/, '');
}

function getTableName(streamName) {
    return `pg_streams_${satinize(streamName)}`
}

function getChannelName(streamName) {
    return `${getTableName(streamName)}_channel`
}

module.exports = { satinize, getTableName, getChannelName };
