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

const Helper = require("./lib/helper"),
    _ = require('lodash');


// TODO: ADD SUPPORT FOR
// 1. Joins: https://questdb.io/docs/reference/sql/join/
// 2. Having https://questdb.io/docs/concept/sql-extensions/#implicit-having/. 
// 3. Other Query Types: Delete, Insert, Update etc.


class Oql extends Helper {
    constructor(opts) {
        super(opts);
        // Init values

        this.timestamp = opts.timestamp || 'timestamp';

    }


    async build(oql, queryType = 'select') {

        let self = this;

        this.__validate('oql', { oql });
        this.__validate('query_type', { queryType });

        // console.log(JSON.stringify(oql, 0, 4));
        // console.log(oql);

        this.hasBuilder = true;

        // return

        let builder = {};

        // A few builder rule enforcements
        // 1. All columns in where clause (match) must be selected (fields)
        let selectFields = _.uniq(oql.fields.map(a => a.join('--')));

        if (oql.match) {
            let conditionFields = _.uniq(oql.match.map(o => o.cond.map(o => o[0])).reduce((a, b) => a.concat(b), []));

            oql.fields = _.concat(oql.fields, _.difference(conditionFields, selectFields).map(v => [v]));
        }

        // 2. You cannot use both latest by & sample by
        if (oql.sampleBy && oql.latestBy) throw new Error('You cannot use "sampleBy" together with "latestBy"')

        // 3. Align to cannot exist without sample by
        if (oql.alignTo && !oql.sampleBy) throw new Error('You cannot have "alignTo" without "sampleTo"')

        // 4. Align cannot be used with limit
        if (oql.alignTo && oql.limit) throw new Error('You cannot have "alignTo" together with "limit"')

        // 5. You cannot use order by with sample by
        if (oql.alignTo && oql.orderBy) throw new Error('You cannot have "alignTo" together with "orderBy"')

        // 6. Not a rule but important to have data sampled as expected
        // If user has sample without align, attempt to add ALIGN BY CALENDAR as default
        if (oql.sampleBy && !oql.alignTo)
            oql.alignTo = "CALENDAR TIME ZONE 'GMT' WITH OFFSET '00:00'";

        // pick values
        builder.table = oql.from;
        this.table = builder.table;

        builder.limit = oql.limit;

        builder.fields = await this.__parse_all_fields(oql.fields);


        builder.match = await this.__format_match(oql.match);

        // default bool is always AND
        builder.bool = oql.bool || "AND";

        if (oql.orderBy)
            builder.orderBy = await Promise.all(oql.orderBy.map(async(a) => {
                a[0] = await self.formatColName(a[0]);
                let ordering = a[1] || "ASC";
                this.__validate("ordering", { ordering })
                a[1] = ordering;
                return a
            }));

        if (oql.distinct)
            builder.distinct = await Promise.all(oql.distinct.map(v => {
                return self.formatColName(v);
            }));

        if (oql.latestBy)
            builder.latestBy = await Promise.all(oql.latestBy.map(v => {
                return self.formatColName(v);
            }));


        // console.log(oql.tsIn.in);
        if (oql.tsIn) {
            builder.tsIn = await this.__escape_string(oql.tsIn);
        }


        builder.sampleBy = oql.sampleBy ? oql.sampleBy.replace(/\s{2,}/g, ' ') : null;
        builder.alignTo = oql.alignTo ? oql.alignTo.replace(/\s{2,}/g, ' ') : null;

        // console.log(oql);

        // build actual string
        this.builder = builder;


        return this[queryType]()

    }


    async select() {

        let self = this;

        if (!this.hasBuilder) throw new Error(`You must first call build(oql) before calling select.`);

        // add timestamp IN condition to where/match
        if (this.builder.tsIn)
            this.builder.match.unshift(` ${this.timestamp} IN (${this.builder.tsIn.join(',')})`)

        // console.log(this.builder);
        // Make SQL
        let SQL = `SELECT `

        // add distinct
        if (this.builder.distinct) {
            SQL += `DISTINCT ` + (await Promise.all(this.builder.distinct.map(async v => {

                if (self.forceAlias) {
                    v = `${v} as ${this.__escape_string(await this.__format_col_names(v,true))}`;
                }

                return v;
            }))).join(', ');

            SQL += `, `;
        }

        // add fields
        SQL += this.builder.fields.length ? this.builder.fields.join(', ') : (this.builder.distinct ? '' : '*');

        // add from 
        SQL += `\n FROM ` + this.builder.table;

        // ADD Latest BY (Bracket Wrapping)
        if (this.builder.latestBy)
            SQL = ` (${SQL} LATEST BY ${this.builder.latestBy.join(', ')})`

        // add WHERE
        if (this.builder.match.length)
            SQL += `\n WHERE ` + this.builder.match.join(` ${this.builder.bool} `);

        // sample by
        if (this.builder.sampleBy)
            SQL += `\n SAMPLE BY ` + this.builder.sampleBy;

        // add align by
        if (this.builder.alignTo)
            SQL += `\n ALIGN TO ` + this.builder.alignTo;

        // add order by
        if (this.builder.orderBy)
            SQL += `\n ORDER BY ` + this.builder.orderBy.map(a => a.join(' ')).join(', ');

        // add group by
        if (this.builder.groupBy)
            SQL += `\n GROUP BY ` + this.builder.groupBy.join(', ');

        // add limit
        if (this.builder.limit)
            SQL += `\n LIMIT ` + this.builder.limit.join(',');


        SQL += `;`


        return SQL;
    }

    async delete() {
        // TODO: Compose delete commands
    }

    async insert() {
        // TODO: Compose insert command
    }

}




module.exports = (opts = {}) => new Oql(opts)