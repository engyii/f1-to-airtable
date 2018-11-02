const axios = require('axios');
const Airtable = require('airtable');
const _ = require('lodash');
const secret = require('./secret.json');

Airtable.configure({
    endpointUrl: 'https://api.airtable.com',
    apiKey: secret.apiKey
});

const f1Base = Airtable.base(secret.db);

const recordCache = {};
const findRecord = function(table, filter) {
    if(!recordCache[table]) {
        recordCache[table] = {};
    } else {
        let cachedRecord = recordCache[table][filter];
        if(cachedRecord) {
            if (cachedRecord.then) return cachedRecord;
            else {
                return Promise.resolve(cachedRecord === 0 ? undefined : cachedRecord); 
            }
        }
    }

    recordCache[table][filter] = f1Base(table).select({
        maxRecords: 1,
        filterByFormula: filter
    }).firstPage().catch(displayErrorMsgs).then(records => { 
        let record = (records && records.length > 0) ? records[0] : 0; 
        recordCache[table][filter]  = record;
        return record;
    });

    return recordCache[table][filter];
};

const displayErrorMsgs = function() {
    [].concat(arguments).forEach(arg => console.error(arg));
}

const flattenObject = function(object, root, prefix) {
    let result = root || {};
    let keyPrefix = (prefix ? prefix + '-': '');
    _.forOwn(object, function(value, key, object) {
        if(_.isObject(value)) {
            flattenObject(value, result, keyPrefix + key);
        } else {
            result[keyPrefix +  key] = value;
        }
    });
 return result;
}

const createOrUpdateRecord = function(record, table, primaryKey, doNotUpdate) {
    return findRecord(table, primaryKey + " = '" + record[primaryKey] + "'").then(existingRecord => {
        if (!existingRecord) {
            console.log("Non-existing record '" + record[primaryKey] + "'");
            f1Base(table).create(record).catch(err => displayErrorMsgs(err, record));
        } else {
            console.log("Existing record '" + record[primaryKey] + "'");
            if (!doNotUpdate) 
                f1Base(table).replace(existingRecord.id, record).catch(err => displayErrorMsgs(err, record));
        }
    })
}

const importDrivers = async function(season) {
    const res = await axios.get('https://ergast.com/api/f1/' + season + '/drivers.json?limit=1000');
    return Promise.all(res.data.MRData.DriverTable.Drivers.map(driver => createOrUpdateRecord(driver, 'Drivers', 'driverId')));
}

const importCircuits = async function(season) {
    const res = await axios.get('http://ergast.com/api/f1/' + season + '/circuits.json?limit=1000');
    return Promise.all(res.data.MRData.CircuitTable.Circuits.map(circuit => createOrUpdateRecord(flattenObject(circuit), 'Circuits', 'circuitId')));
}

const importConstructors = async function(season) {
    const res = await axios.get('http://ergast.com/api/f1/' + season + '/constructors.json?limit=1000');
    return Promise.all(res.data.MRData.ConstructorTable.Constructors.map(constructor => createOrUpdateRecord(constructor, 'Constructors', 'constructorId')));
}

const importRaces = async function(season) {
    const res = await axios.get('http://ergast.com/api/f1/' + season + '.json?limit=1000');
    return Promise.all(res.data.MRData.RaceTable.Races.map(race => {
        return findRecord('Circuits', "circuitId = '" + race.Circuit.circuitId + "'").then(circuit => 
            createOrUpdateRecord(
                _(race).pick('season', 'round', 'url', 'raceName', 'date').merge({circuit: [circuit.id], raceId: getRaceId(race) }).value(),
                'Races', 
                'raceId'
            )
        );   
    }));
}

const getRaceId = function(race) {
    return race.season + '/' + _.padStart(race.round, 2, '0');
}

const importResults = async function(season) {
    const res = await axios.get('http://ergast.com/api/f1/' + season + '/results.json?limit=1000');
    return Promise.all(_.flatten(res.data.MRData.RaceTable.Races.map(async (race) => {
        
        let currentCircuit = await findRecord('Circuits', "circuitId = '" + race.Circuit.circuitId + "'");
        let currentRace = await findRecord('Races', "raceId = '" + getRaceId(race) + "'");

        return race.Results.map(async (result, index) => {
            let resultId = getRaceId(race) + '/' + _.padStart(index, 2, '0');
            let currentDriver = await findRecord('Drivers', "driverId = '" + result.Driver.driverId + "'");
            let currentConstructor = await findRecord('Constructors', "constructorId = '" + result.Constructor.constructorId + "'");

            let record = flattenObject(_.pick(result, 'position', 'positionText', 'points', 'grid', 'laps', 'status', 'Time', 'FastestLap'))
            _.merge(record, { resultId: resultId, circuit: [currentCircuit.id], driver: [currentDriver.id], race: [currentRace.id], constructor: [currentConstructor.id] });
            return createOrUpdateRecord(record, 'Results', 'resultId');

        });
    })));
}

async function importSeason(season) {
    //order is important

    // await importDrivers(season);
    // console.log('Drivers imported');

    // await importCircuits(season);
    // console.log('Circuits imported');

    // await importConstructors(season);
    // console.log('Constructors imported');

    // await importRaces(season);
    // console.log('Races imported');

    await importResults(season);
    console.log('Results imported');
}

importSeason(2018);


