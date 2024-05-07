// src/index.js

const fs = require('fs');
const parseQuery = require('./queryParser');

function readCSV(filename) {
    return new Promise((resolve, reject) => {
        fs.readFile(filename, 'utf8', (err, data) => {
            if (err) {
                reject(err);
                return;
            }
            const lines = data.trim().split('\n');
            const headers = lines[0].split(',');
            const records = lines.slice(1).map(line => {
                const record = {};
                line.split(',').forEach((value, index) => {
                    record[headers[index].trim()] = value.trim();
                });
                return record;
            });
            resolve(records);
        });
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    const fieldValue = row[field];
    switch (operator) {
        case '=':
            return fieldValue === value;
        case '>':
            return fieldValue > value;
        case '<':
            return fieldValue < value;
        case '!=':
            return fieldValue !== value;
        default:
            throw new Error('Unsupported operator');
    }
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            return joinData
                .filter(joinRow => {
                    const mainValue = mainRow[joinCondition.left.split('.')[1]];
                    const joinValue = joinRow[joinCondition.right.split('.')[1]];
                    return mainValue === joinValue;
                })
                .map(joinRow => {
                    return fields.reduce((acc, field) => {
                        const [tableName, fieldName] = field.split('.');
                        acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                        return acc;
                    }, {});
                });
        });
    }

    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

module.exports = executeSELECTQuery;
