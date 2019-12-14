require('dotenv').config();
const _ = require('lodash');
const debug = require('debug')("prm2:tp1:exo1");
const parse = require("csv-parse");
const fs = require('fs');
const { min, max, median, mean } = require('simple-statistics');

const parser = parse({
    delimiter: ";",
    columns: true,
    skip_empty_lines: true,
    trim: true
});
const data = fs.readFileSync(__dirname + '/data.csv', 'utf8');
let store = [];
const duplicates = [];
const annualClient = [];
const semesterClient = [];
const trimesterClient = [];
const recenceLT3Client = [];
const duplicateCounter = {};
const sexCounter = {
    F: 0,
    M: 0
};

parser.on('readable', function reading() {
    let record;
    while (record = parser.read()) {
        const {compte_client, age_client, recence, sexe} = record;

        store.push(record);
        if (!_.has(duplicateCounter, compte_client)) {
            _.set(duplicateCounter, compte_client, 0);
        }
        _.set(duplicateCounter, compte_client, _.get(duplicateCounter, compte_client) + 1);
    }
});
// Catch any error
parser.on('error', function error(err) {
    console.error(err.message)
});
// When we are done, test that the parsed output matched what expected
parser.on('end', function end() {
    const initialLength = store.length;

    Object.keys(duplicateCounter).forEach(key => {
        if (_.get(duplicateCounter, key) > 1) {
            duplicates.push(
                ..._.filter(store, ({compte_client}) => compte_client === key)
            );
            store = _.filter(store, ({compte_client}) => compte_client !== key);
        }
    });
    debug("Nombre de doublons : %d", initialLength - Object.keys(duplicateCounter).length);
    debug("Liste des comptes clients en doublons: ");
    _.forEach(
        _.uniqBy(duplicates, 'compte_client'),
        ({compte_client}) => debug(compte_client)
    );

    _.forEach(store, item => {
        const { sexe, recence } = item;

        if ('F' === sexe)
            _.set(sexCounter, 'F', _.get(sexCounter, 'F') + 1);
        else if ('M' === sexe)
            _.set(sexCounter, 'M', _.get(sexCounter, 'M') + 1);

        if (recence <= 12)
            annualClient.push(item);

        if (recence <= 6)
            semesterClient.push(item);

        if (recence <= 3)
            trimesterClient.push(item);
    });

    debug("Nombre de clients de sexe M : %d", sexCounter.M);
    debug("Nombre de clients de sexe F : %d", sexCounter.F);
    debug("Nombre de clients dont les achats de l’année sont non-nuls : %d", annualClient.length);
    debug("Nombre de clients dont les achats du semestre sont non-nuls : %d", semesterClient.length);
    debug("Nombre de clients dont les achats du trimestre sont non-nuls : %d", trimesterClient.length);
    const ages = _.map(store, ({age_client}) => parseInt(age_client, 10));
    debug("L'âge minimum des clients : %d", min(ages));
    debug("L'âge maximum des clients : %d", max(ages));
    debug("L'âge moyen des clients : %d", mean(ages));
    debug("L'âge médian des clients : %d", median(ages));
});
// Write data to the stream
parser.write(data);
// Close the readable stream
parser.end();
