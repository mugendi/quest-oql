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



let _fieldSchema = {
        type: "array",
        items: {
            type: "array",
            min: 1,
            max: 3,
            items: [{
                // pattern: /[a-z0-9]/i,
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
    fieldArraySchema = { type: "array", min: 1, items: singleFieldSchema },
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
                from: {
                    type: 'string',
                    pattern: /^[a-z_][a-z0-9_]+$/i,
                    messages: {
                        stringPattern: "From contains characters that are not allowed."
                    }
                },
                bool: boolSchema,
                match: is_optional(_matchSchema),

                distinct: optionalFieldArraySchema,

                orderBy: {
                    type: "array",
                    optional: true,
                    items: {
                        type: "array"
                    }
                },

                // TODO: There seems to be a bug
                // latestBy: {
                //     type: "array",
                //     max: 2,
                //     optional: true,
                //     items: {
                //         type: "string"
                //     }
                // },

                sampleBy: {
                    type: "string",
                    optional: true,
                    trim: true,
                    pattern: /^[0-9]+[TsmhdM]$/,
                    messages: {
                        stringPattern: "SampleBy is incorrect. Should be a string like '1h'."
                    }
                },
                alignTo: {
                    type: "string",
                    optional: true,
                    trim: true,
                    pattern: /^FIRST\s+OBSERVATION|(CALENDAR\s*(WITH\s+OFFSET|TIME\s+ZONE\s+'.+')?)$/i,
                    messages: {
                        stringPattern: "AlignTo is incorrect."
                    }
                },
                limit: {
                    type: "array",
                    max: 2,
                    optional: true,
                    items: {
                        type: "number"
                    }
                },
                tsIn: {
                    type: "array",
                    optional: true,
                    items: {
                        type: "string"
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

            enum: [
                "min",
                "max",
                "avg",
                "sum",
                "nsum",
                "ksum",
                "first",
                "last",
                "haversine_dist_deg",
                "count",
                "count_distinct"
            ],
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
    },

    field_name: {
        fieldName: {
            type: "string",
            pattern: /^[a-z0-9_\*\(\)]+$/i,
            messages: {
                stringPattern: "Field name contains characters that are not allowed."
            }
        }
    }
}


function is_optional(schema) {
    return Object.assign(schema, { optional: true })
}


module.exports = schemas