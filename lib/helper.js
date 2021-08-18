// Copyright 2021 Anthony Mugendi
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const Validator = require("fastest-validator"),
    v = new Validator(),
    SqlString = require('sqlstring'),
    format = require('util').format,
    schemas = require('./schema')




class Helper {

    constructor(opts = {}) {

        // set field formatter
        this.formatColName = typeof opts.formatColName == 'function' ? opts.formatColName : (f) => f;
        this.forceAlias = opts.forceAlias !== undefined ? opts.forceAlias : false;

    }

    __validate(name, obj, type = "select") {

        const schema = schemas[type][name],
            check = v.compile(schema),
            valid = check(obj);

        if (valid !== true) {
            let messages = valid.map(o => o.message);
            var err = new Error(format('Could not validate: "%s" \n\t %s', type, messages.join('\n\t')));
            throw err
        }

    }

    __escape_string(val) {
        let self = this;

        if (Array.isArray(val))
            return val.map(self.__escape_string)

        return SqlString.escape(val);
    }

    async __format_match(match) {
        this.__validate('matchSchema', { match });

        if (!match) return [];

        // console.log(match);

        let matchConditions = [];

        // loop thru
        for (let matchObj of match) {

            let { bool, cond } = matchObj;

            // default boolean is always AND
            bool = bool || 'AND';

            let condArray = await this.__parse_fields_arr(cond, 'match_condition');

            matchConditions.push(condArray.join(` ${bool} `));
        }

        return matchConditions;
    }

    async __format_all_fields(fields) {
        return Promise.all(arrify(fields).map(this.__format_col_names));
    }

    async __format_col_names(col, asAlias = false) {

        // do not format fields starting with a ^
        if (/^\^/.test(col)) return col.slice(1);

        return await this.formatColName(col, asAlias);

    }

    async __parse_fields_arr(fields, type = 'aggregation') {

        let formattedFields = [];

        // loop && format
        for (let fieldArr of arrify(fields)) {

            let fieldStr;

            // get name, operation && param
            let [fieldName, op, param] = fieldArr;
            fieldName = fieldName.toString();

            // validate by type function
            this.__validate(type, {
                [type]: op
            });

            // uppercase operation
            // op = op && op.toUpperCase();

            // format field names
            fieldStr = await this.__format_col_names(fieldName);

            // if we have an op
            if (op) {
                // for aggregations 
                if (type == 'aggregation') {
                    fieldStr = `${op}(${fieldStr})`;
                } else if (type == 'match_condition') {
                    fieldStr = `${fieldStr} ${op} ${SqlString.escape(param)}`;
                }
            }


            // if has param
            if (param && type == 'aggregation') {
                fieldStr = `${fieldStr} as ${SqlString.escape(await this.__format_col_names(param,true))}`
            } else if (!param && this.forceAlias) {
                fieldStr = `${fieldStr} as ${SqlString.escape(await this.__format_col_names(fieldStr,true))}`
            }


            formattedFields.push(fieldStr);

        }

        return formattedFields;
    }

    async __parse_all_fields(fields) {

        this.__validate('fieldSchema', { fields });

        let formattedFields = await this.__parse_fields_arr(fields);

        return formattedFields;

    }

}


function arrify(v) {
    if (!v) return [];
    return Array.from(v) ? v : [v];
}


module.exports = Helper;