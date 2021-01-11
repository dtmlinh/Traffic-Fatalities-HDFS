'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

// HBase counters are stored as 8 byte binary data that the HBase Node module
// interprets as an 8 character string. Use the Javascript Buffer library to
// convert into a number
function counterToNumber(c) {
    try {
        return Number(Buffer.from(c).readBigInt64BE());
    }
    catch (e) {
        return Number(c);
    }
}

function rowToMap(row) {
    var stats = {}
    row.forEach(function (item) {
        stats[item['column']] = counterToNumber(item['$'])
    });
    return stats;
}

app.use(express.static('public'));
app.get('/accidents.html', function (req, res) {
    hclient.table('ldinh_state').scan({maxVersions: 1}, (err,rows) => {
        var template = filesystem.readFileSync("accidents.mustache").toString();
        var html = mustache.render(template, {
            states : rows
        });
        res.send(html)
    })
});

function removePrefix(text, prefix) {
    if(text.indexOf(prefix) != 0) {
        throw "missing prefix"
    }
    return text.substr(prefix.length)
}

app.get('/accidents_by_state.html',function (req, res) {
    const state=req.query['state'];
    console.log(state);
    function processYearRecord(yearRecord) {
        console.log(yearRecord)
        var result = { year : yearRecord['year']};
        var overall_acc = yearRecord["tot_acc"];
        var overall_fat = yearRecord["tot_fat"];

        if (overall_acc > overall_fat && overall_acc > 365) {
            ["tot", "daytime", "nightime", "clear", "rain", "snow", "cloudy", "fog", "hail", "junction",
                "mon", "tue", "wed", "thu", "fri", "sat", "sun",
                "hour_1", "hour_2", "hour_3", "hour_4", "hour_5", "hour_6",
                "hour_7", "hour_8", "hour_9", "hour_10", "hour_11", "hour_12",
                "hour_13", "hour_14", "hour_15", "hour_16", "hour_17", "hour_18",
                "hour_19", "hour_20", "hour_21", "hour_22", "hour_23", "hour_24",
                "month_1", "month_2", "month_3", "month_4", "month_5", "month_6",
                "month_7", "month_8", "month_9", "month_10", "month_11", "month_12"].forEach((factor, formatter) => {
                var accidents = yearRecord[factor + "_acc"]
                var fatalities = yearRecord[factor + "_fat"]
                if (accidents === 0 || typeof accidents == "undefined" || typeof fatalities == "undefined" || fatalities > accidents) {
                    result[factor + '_raw'] = "-"
                } else {
                    result[factor + '_raw'] = accidents.toLocaleString('en')
                }

                if (accidents === 0 || typeof accidents == "undefined" || typeof fatalities == "undefined" || fatalities > accidents) {
                    result[factor] = "-"
                } else {
                    result[factor] = (100 * fatalities / accidents).toFixed(1) + '%';
                }
            })
        }
        else {
            ["tot", "daytime", "nightime", "clear", "rain", "snow", "cloudy", "fog", "hail", "junction",
            "mon", "tue", "wed", "thu", "fri", "sat", "sun",
            "hour_1", "hour_2", "hour_3", "hour_4", "hour_5", "hour_6",
            "hour_7", "hour_8", "hour_9", "hour_10", "hour_11", "hour_12",
            "hour_13", "hour_14", "hour_15", "hour_16", "hour_17", "hour_18",
            "hour_19", "hour_20", "hour_21", "hour_22", "hour_23", "hour_24",
            "month_1", "month_2", "month_3", "month_4", "month_5", "month_6",
            "month_7", "month_8", "month_9", "month_10", "month_11", "month_12"].forEach((factor, formatter) => {
                result[factor + '_raw'] = "-"
                result[factor] = "-"
            })
        }

        if (typeof yearRecord['avg_hosp_arr_mn'] == "undefined" ) {
            result['avg_hosp_arr_mn'] = "-"
        }
        else {
            result['avg_hosp_arr_mn'] = (yearRecord['avg_hosp_arr_mn']);
        }

        if (typeof yearRecord['avg_hosp_5_mi'] == "undefined" ) {
            result['avg_hosp_5_mi'] = "-"
        }
        else {
            result['avg_hosp_5_mi'] = (yearRecord['avg_hosp_5_mi']);
        }

        if (yearRecord['tot_sp'] === 0 || typeof yearRecord['hea_sp'] == "undefined" || typeof yearRecord['hos_sp'] == "undefined" || typeof yearRecord['hig_sp'] == "undefined") {
            result['hlth_shr'] = "-"
            result['high_shr'] = '-'
        }
        else {
            result['hlth_shr'] = (100*(yearRecord['hea_sp'] + yearRecord['hos_sp'])/yearRecord['tot_sp']).toFixed(1)+'%';
            result['high_shr'] = (100*yearRecord['hig_sp']/yearRecord['tot_sp']).toFixed(1)+'%';
        }

        return result;
    }
    function stateInfo(cells) {
        var result = [];
        var yearRecord;
        cells.forEach(function(cell) {
            var year = Number(removePrefix(cell['key'], state))
            if(yearRecord === undefined)  {
                yearRecord = { year: year }
            } else if (yearRecord['year'] != year ) {
                result.push(processYearRecord(yearRecord))
                yearRecord = { year: year }
            }
            const col_n = removePrefix(cell['column'],'acc:')
            if (col_n !== 'avg_hosp_arr_min' && col_n !== 'avg_hosp_arr_min' && col_n !== 'tot_sp' && col_n !== 'hea_sp' && col_n !== 'hos_sp' && col_n !== 'hig_sp') {
                yearRecord[col_n] = counterToNumber(cell['$'])
            }
            else {
                yearRecord[col_n] = Number(cell['$'])
            }
        })
        result.push(processYearRecord(yearRecord))
        console.info(result)
        return result;
    }

    hclient.table('ldinh_accidents_conditions_by_state').scan({
            filter: {type : "PrefixFilter",
                value: state},
            maxVersions: 1},
        (err, cells) => {
            var ai = stateInfo(cells);
            var template = filesystem.readFileSync("results.mustache").toString();
            var html = mustache.render(template, {
                stateInfo : ai,
                state : state
            });
            res.send(html)

        })
});

app.listen(port);
