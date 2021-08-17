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
    format = require('util').format;


let _fieldSchema = {
        type: "array",
        items: {
            type: "array",
            min: 1,
            max: 3,
            items: [{
                type: "string",
                optional: true
            }]
        }
    },
    boolSchema = { type: "string", uppercase: true, enum: ["AND", "OR"], optional: true },
    _matchSchema = {
        type: "array",
        items: {
            type: "object",
            strict: "remove",
            props: {
                bool: boolSchema,
                cond: {
                    type: "array",
                    items: {
                        type: "array",
                        min: 3,
                        max: 3,
                        items: [
                            { type: "string" },
                            { type: "number" }
                        ]
                    }
                }
            }
        }
    },
    singleFieldSchema = { type: "string" },
    optionalSingleFieldSchema = is_optional(singleFieldSchema),
    fieldArraySchema = { type: "array", items: singleFieldSchema },
    optionalFieldArraySchema = is_optional(fieldArraySchema)


let schemas = {
    opts: {},
    fieldSchema: {
        fields: _fieldSchema
    },
    matchSchema: {
        match: _matchSchema
    },

    oql: {
        oql: {
            type: "object",
            strict: "remove",
            props: {
                fields: is_optional(_fieldSchema),
                from: { type: 'string', pattern: /^[a-z_][a-z0-9_]+$/i },
                bool: boolSchema,
                match: is_optional(_matchSchema),
                groupBy: optionalFieldArraySchema,
                distinct: optionalFieldArraySchema,
                orderBy: {
                    type: "array",
                    optional: true,
                    items: {
                        type: "array"
                    }
                },
                latestBy: optionalFieldArraySchema,
                sampleBy: {
                    type: "string",
                    optional: true,
                    trim: true,
                    pattern: /^[0-9]+[TsmhdM]$/
                },
                alignTo: {
                    type: "string",
                    optional: true,
                    trim: true,
                    pattern: /^FIRST\s+OBSERVATION|(CALENDAR\s*(WITH\s+OFFSET|TIME\s+ZONE\s+'.+')?)$/i
                },
                limit: {
                    type: "array",
                    max: 2,
                    optional: true,
                    items: {
                        type: "number"
                    }
                }
            }
        }
    },
    aggregation: {
        aggregation: {
            type: "string",
            optional: true,
            lowercase: true,

            enum: ["min", "max", "avg", "sum"],
        }
    },

    ordering: {
        ordering: {
            type: "string",
            uppercase: true,
            enum: ["DESC", "ASC"]
        }
    },

    match_condition: {
        match_condition: {
            type: "string",
            optional: true,
            uppercase: true,

            enum: ["=", ">", "<", "<=", ">=", "LIKE"],
        }
    },

    query_type: {
        queryType: {
            type: "string",
            enum: ["select", "update", "insert", "delete"]
        }
    }
}

function is_optional(schema) {
    return Object.assign(schema, { optional: true })
}

class Helper {

    constructor(opts = {}) {

        // set field formatter
        this.formatFields = typeof opts.formatFields == 'function' ? opts.formatFields : (f) => f

    }

    __validate(type, obj) {

        const schema = schemas[type],
            check = v.compile(schema),
            valid = check(obj);

        if (valid !== true) {
            let messages = valid.map(o => o.message);
            var err = new Error(format('Could not validate: "%s" \n\t %s', type, messages.join('\n\t')));
            throw err
        }

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
        return Promise.all(arrify(fields).map(this.formatFields));
    }


    async __parse_fields_arr(fields, type = 'aggregation') {

        let formattedFields = [];

        // loop && format
        for (let fieldArr of arrify(fields)) {

            let fieldStr;

            // get name, operation && param
            let [fieldName, op, param] = fieldArr;

            // validate by type function
            this.__validate(type, {
                [type]: op
            });

            // uppercase operation
            // op = op && op.toUpperCase();

            // format field names
            fieldStr = await this.formatFields(fieldName.toString());

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
            if (param && type == 'aggregation') fieldStr = `${fieldStr} as ${param}`


            formattedFields.push(fieldStr);

        }

        return formattedFields;
    }

    async __format_fields(fields) {

        this.__validate('fieldSchema', { fields });

        let formattedFields = this.__parse_fields_arr(fields);




        return formattedFields;

    }

}


function arrify(v) {
    if (!v) return [];
    return Array.from(v) ? v : [v];
}


module.exports = Helper;