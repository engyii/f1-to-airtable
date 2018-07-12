const axios = require('axios');
const Airtable = require('airtable');

Airtable.configure({
    endpointUrl: 'https://api.airtable.com',
    apiKey: 'keyI5Mqk4ff9Xk3yU'
});

const f1Base = Airtable.base('app6brjk7W96JieLL');

const findRecord = function(table, filter) {
    return f1Base(table).select({
        maxRecords: 1,
        filterByFormula: filter
    }).firstPage().catch(displayMsgs).then(records => { if (records.length > 0) return records[0]; })
}

const displayErrorMsgs = function() {
    arguments.forEach(arg => console.error(arg));
}

const createOrUpdateRecord = function(record, table, primaryKey) {
    findRecord(table, primaryKey + " = '" + record[primaryKey] + "'").then(existingRecord => {
        if (!existingRecord) {
            console.log("Non-existing record '" + record[primaryKey] + "'");
            f1Base(table).create(record).catch(err => displayErrorMsgs(err, record));
        } else {
            console.log("Existing record '" + record[primaryKey] + "'");
            f1Base(table).replace(existingRecord.id, record).catch(err => displayErrorMsgs(err, record));
        }
    })
}

const importAllDrivers = async function() {
    const res = await axios.get('https://ergast.com/api/f1/drivers.json?limit=1000');
    res.data.MRData.DriverTable.Drivers.forEach(driver => createOrUpdateRecord(driver, 'Drivers', 'driverId'));
}

const importAllCircuits = async function() {
    const res = await axios.get('http://ergast.com/api/f1/circuits.json?limit=1000')
}

importAllDrivers();