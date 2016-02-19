"use strict";

const wadofgum = require('wadofgum');
const wadofgumValidation = require('wadofgum-validation');
const wadofgumProcess = require('wadofgum-process');

const lodash = require('lodash');
const assert = require('assert');
const Joi = require('joi');
const mssql = require('mssql');

const methodValidators = {
  mapProcedure: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    output: Joi.array().length(2),
    oneResult: Joi.boolean().default(false),
    resultModels: Joi.array(Joi.string())
  }),
  mapStatement: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    args: Joi.array().items(Joi.array().length(2)),
    output: Joi.object().unknown(),
    query: Joi.func().required(),
    oneResult: Joi.boolean().default(false)
  }),
  mapQuery: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    query: Joi.func().required(),
    oneResult: Joi.boolean().default(false)
  })
};

const dataTypes = {
  bigint: mssql.BigInt,
  numeric: mssql.Numeric,
  bit: mssql.Bit,
  smallint: mssql.SmallInt,
  decimal: mssql.Decimal,
  smallmoney: mssql.SmallMoney,
  int: mssql.Int,
  tinyint: mssql.TinyInt,
  money: mssql.Money,
  float: mssql.Float,
  real: mssql.Real,
  date: mssql.Date,
  datetimeoffset: mssql.DateTimeOffset,
  datetime2: mssql.DateTime2,
  smalldatetime: mssql.SmallDateTime,
  datetime: mssql.DateTime,
  time: mssql.Time,
  char: mssql.Char,
  varchar: mssql.VarChar,
  text: mssql.Text,
  nchar: mssql.NChar,
  nvarchar: mssql.NVarChar,
  ntext: mssql.NText,
  binary: mssql.Binary,
  varbinary: mssql.VarBinary,
  image: mssql.Image
};

const dataCall = {
  decimal: ['NUMERIC_PRECISION', 'NUMERIC_SCALE'],
  numeric: ['NUMERIC_PRECISION', 'NUMERIC_SCALE'],
  char: ['CHARACTER_MAXIMUM_LENGTH'],
  nchar: ['CHARACTER_MAXIMUM_LENGTH'],
  varchar: ['CHARACTER_MAXIMUM_LENGTH'],
  nvarchar: ['CHARACTER_MAXIMUM_LENGTH'],
  time: ['NUMERIC_SCALE'],
  datetime2: ['NUMERIC_SCALE'],
  datetimeoffset: ['NUMERIC_SCALE'],
  varbinary: ['CHARACTER_MAXIMUM_LENGTH']
};

const dataUserCall = {
  decimal: ['precision', 'scale'],
  numeric: ['precision', 'scale'],
  char: ['max_length'],
  nchar: ['max_length'],
  varchar: ['max_length'],
  nvarchar: ['max_length'],
  time: ['scale'],
  datetime2: ['scale'],
  datetimeoffset: ['scale'],
  varbinary: ['max_length']
};


function EmptyResult() {
  Error.apply(this, arguments);
  this.message = "EmptyResult";
}

EmptyResult.prototype = Object.create(Error.prototype);
    
const TVP = (types) => {
  return {type: 'TVP', types};
};

let getDB;
const cached_models = {};

class Model extends wadofgum.mixin(wadofgumValidation, wadofgumProcess) {

  constructor (opts) {
    super(opts);
    this.map = this.map || {};

    if (this.name) {
      cached_models[this.name] = this;
    }

    this._preparedStatements = {};

    this.getDB = getDB;

    this.unprepare = function unprepare(name) {
      return this._preparedStatements[name].unprepare();
    };

    this.mssql = null;
  }

  setView (vname) {
    this.tableDefinition = {};
    this.tableName = vname;
    return this.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      return new Promise((resolve, reject) => {
        request.query(`
          SELECT 
          c.name as colName,
          st.name AS colType,
          c.*
          FROM sys.columns c
          INNER JOIN sys.views v ON c.object_id = v.object_id
          INNER JOIN sys.systypes st ON st.xtype = c.system_type_id AND st.name != 'sysname'
          WHERE v.name = '${vname}'`, (err, result) => {
          if (err) {
            return reject(err);
          }
          if (result.length === 0) {
            reject(new Error(`No columns found for view ${vname}`));
          }
          const coltypes = [];
          for (const row of result) {
            if (dataUserCall.hasOwnProperty(row.colType)) {
              const callArgs = dataUserCall[row.colType].map(col => row[col]);
              this.tableDefinition[row.colName] =  dataTypes[row.colType].apply(mssql, callArgs);
            } else {
              this.tableDefinition[row.colName] =  dataTypes[row.colType];
            }
          }
          this.createQueries(true);
          return resolve();
        })
      });
    })
  }

  setTable (tname) {
    return this.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      return new Promise((resolve, reject) => {
        request.query(`SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '${tname}'`, (err, r) => {
          if (err) return reject(err);
          return resolve(r);
        })
      });
    })
    .then((r) => {
      if (r.length === 0) {
        throw new Error(`No columns found for table ${tname}`);
      }
      this.tableDefinition = {};
      this.tableName = tname;
      for (const row of r) {
        if (typeof dataCall[row.DATA_TYPE] !== 'undefined') {
          const callArgs = dataCall[row.DATA_TYPE].map((col) => {
            return row[col];
          });
          this.tableDefinition[row.COLUMN_NAME] = dataTypes[row.DATA_TYPE].apply(mssql, callArgs);
        } else {
          this.tableDefinition[row.COLUMN_NAME] = dataTypes[row.DATA_TYPE];
        }
      }
      this.createQueries();
    });
  }

  _getPreparedArgs(args) {
    args = args || {};
    return this.getDB()
    .then((db) => {
      const request = new mssql.PreparedStatement(db);
      return this.validateAndProcess(args)
      .then((args) => {
        return Promise.resolve({ps: request, args});
      });
    })
    .then((psAndArgs) => {
      const args = psAndArgs.args;
      const ps = psAndArgs.ps;
      const keys = Object.keys(args);
      for (const key of keys) {
        ps.input(key, this.tableDefinition[key], args[key]);
      }
      return Promise.resolve({ps, args, keys});
    });
  }
  
  _queryTable(ps, args, query) {
    return new Promise((resolve, reject) => {
      ps.prepare(query, (err, recordset) => {
        /* $lab:coverage:off$ */
        if (err) {
          return reject(err);
        }
        /* $lab:coverage:on$ */
        resolve();
      })
    })
    .then(() => {
      return ps.execute(args)
      .then((recordset) => {
        ps.unprepare();
        return Promise.resolve(recordset);
      });
    });
  }

  createQueries(onlySelect) {

    this.select = (args) => {
      return this._getPreparedArgs(args)
      .then((psArgsKeys) => {
        const args = psArgsKeys.args;
        const ps = psArgsKeys.ps;
        const keys = psArgsKeys.keys;
        let query = `SELECT * FROM [${this.tableName}]`;
        if (keys.length > 0) {
          query += ` WHERE `
          query += keys.map(key => `[${key}] = @${key}`).join(' AND ');
        }
        return this._queryTable(ps, args, query)
        .then((recordset) => {
          return Promise.resolve(this._queryResults({}, recordset));
        });
      });
    };
    
    if (!onlySelect) {
      this.update = (args, where) => {
        return this.validateAndProcess(where)
        .then((where) => {
          return this._getPreparedArgs(args)
          .then((psArgsKeys) => {
            const args = psArgsKeys.args;
            const ps = psArgsKeys.ps;
            const keys = psArgsKeys.keys;
            const whereKeys = Object.keys(where);
            for (const key of whereKeys) {
              ps.input(key, this.tableDefinition[key], args[key]);
            }
            let query = `UPDATE [${this.tableName}] SET `;
            query += keys.map(key => `[${key}] = @${key}`).join(', ');
            query += ` WHERE `
            query += whereKeys.map(key => `[${key}] = @${key}`).join(' AND ');
            const combo = lodash.assign(args, where);
            return this._queryTable(ps, combo, query)
            .then((recordset) => {
              return Promise.resolve();
            });
          });
        });
      };
    }
    
    if (!onlySelect) {
      this.delete = (args) => {
        return this._getPreparedArgs(args)
        .then((psArgsKeys) => {
          const args = psArgsKeys.args;
          const ps = psArgsKeys.ps;
          const keys = psArgsKeys.keys;
          let query = `DELETE  FROM [${this.tableName}]`;
          query += ` WHERE `
          query += keys.map(key => `[${key}] = @${key}`).join(' AND ');
          return this._queryTable(ps, args, query);
        });
      };
    }

    if (!onlySelect) {
      this.insert = (args) => {
        return this._getPreparedArgs(args)
        .then((psArgsKeys) => {
          const args = psArgsKeys.args;
          const ps = psArgsKeys.ps;
          const keys = psArgsKeys.keys;
          let query = `INSERT INTO [${this.tableName}] (
            ${keys.join(', ')} ) VALUES (${keys.map(key => '@' + key).join(', ')})`;
          return this._queryTable(ps, args, query);
        });
      }
    }
  }

  validateAndProcess(obj, tags) {
    if (this.schema) {
      return this.validate(obj)
      .then((obj2) => {
        return this.process(obj2, tags);
      });
    } else {
      return this.process(obj, tags);
    }
  }
  
  getModel (mname) {
    if (mname instanceof Model) {
      return mname;
    }
    if (!cached_models.hasOwnProperty(mname)) {
      throw new Error(`The model "${mname}" doesn't exist.`);
    }
    return cached_models[mname];
  }

  mapQuery (opts) {
    const optsValid = methodValidators.mapQuery.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    return this._mapInstanceQuery(opts);
  }

  _mapInstanceQuery (opts) {
    const extension = {};
    this[opts.name] = (obj, args) => {

      return this.getDB()
      .then((db) => {

        const request = new mssql.Request(db);

        let pm;
        if (!opts.static) {
          pm = this.validateAndProcess(obj, 'toDB');
        } else {
          pm = Promise.resolve(obj);
        }
        return pm.then((input) => {
          input = lodash.assign(input, args);
          const qstring = opts.query(input, this);
          return new Promise((resolve, reject) => {
            request.query(qstring, (err, recordset) => {
              /* $lab:coverage:off$ */
              if (err) {
                return reject(err);
              }
              /* $lab:coverage:on$ */
              return resolve(this._queryResults(opts, recordset, undefined));
            });
          });
        });
      });
    }
  }

  mapStatement (opts) {
    const optsValid = methodValidators.mapStatement.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    opts.args = opts.args || [];
    return this._mapInstanceStatement(opts);
  }

  _mapInstanceStatement(opts) {
    const extension = {};
    this[opts.name] = function (obj) {
      let pm;
      if (!opts.static) {
        pm = this.validateAndProcess(obj, 'toDB');
      } else {
        pm = Promise.resolve(obj);
      }
      return pm.then((input) => {
        return new Promise((resolve, reject) => {
          this._preparedStatements[opts.name].execute(input, (err, recordset, returnValue) => {
            if (err) {
              return reject(err);
            }
            return resolve(this._queryResults(opts, recordset, returnValue));
          });
        });
      });
    };
    return this._prepare(opts);
  };

  _queryResults (opts, recordset, returnValue) {
    if (opts.oneResult) {
      if (recordset.length === 0 || recordset[0].length === 0) {
        return Promise.reject(new EmptyResult);
      } else {
        return this.validateAndProcess(recordset[0], 'fromDB');
      }
    }
    const results = [];
    recordset.forEach((row) => {
      results.push(this.validateAndProcess(row, 'fromDB'));
    });
    return Promise.all(results);
  }

  _prepare(opts) {
    return this.getDB()
    .then((db) => {
      const statement = new mssql.PreparedStatement(db);
      opts.args.forEach((arg) => {
        statement.input(arg[0], arg[1]);
      });
      return new Promise((resolve, reject) => {
        this._preparedStatements[opts.name] = statement;
        statement.prepare(opts.query(), (err) => {
          if (err) {
            return reject(err);
          }
          return resolve();
        });
      });
    });
  }

  mapProcedure(opts) {
    const optsValid = methodValidators.mapProcedure.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    opts.resultModels = opts.resultModels || [this];
    return this.mapStaticProc(opts);
  }

  _makeRequest (db, args, opts) {
    const request = new mssql.Request(db);
    opts.args.forEach((arg) => {
      const argdef = arg[1];
      const field = arg[0];
      if (argdef.hasOwnProperty('type') && argdef.type === 'TVP') {
        let tvp = new mssql.Table();
        argdef.types.forEach((mtype) => {
          tvp.columns.add(mtype[0], mtype[1]);
        });
        if (Array.isArray(args[field])) {
          args[field].forEach((argrow) => {
            let row = [];
            argdef.types.forEach((mtype) => {
              let col = mtype[0];
              row.push(argrow[col]);
            });
            tvp.rows.add.apply(tvp.rows, row);
          });
        }
        request.input(field, tvp);
      } else {
        request.input(field, argdef, args[field]);
      }
    });
    return request;
  }

  _getDataType (row) {
    if (!dataTypes[row.DATA_TYPE]) {
      return;
    }
    if (dataCall[row.DATA_TYPE]) {
      const callArgs = dataCall[row.DATA_TYPE].map((col) => {
        return row[col];
      });
      return [row.PARAMETER_NAME.slice(1), dataTypes[row.DATA_TYPE].apply(mssql, callArgs)];
    } else {
      return [row.PARAMETER_NAME.slice(1), dataTypes[row.DATA_TYPE]];
    }
  }

  mapStaticProc(opts) {
    let oRequest;
    return this.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      oRequest = request;
      request.multiple = true;
      return Promise.resolve(request.query(`SELECt * FROM information_schema.parameters where specific_name='${opts.name}' ORDER BY ORDINAL_POSITION`), request);
    })
    .then((result, request) => {
      const args = [];
      const pms = [];
      for (const row of result[0]) {
        const def = this._getDataType(row);
        if (def) {
          pms.push(Promise.resolve(def));
        } else {
          pms.push(
            new Promise((resolve, reject) => {
              oRequest.query(`
select tt.name AS table_Type,
c.name as colName,
st.name AS colType,
c.*,
st.*,
tt.*
from sys.table_types tt
inner join sys.columns c on c.object_id = tt.type_table_object_id
INNER JOIN sys.systypes AS ST  ON ST.xtype = c.system_type_id AND st.name != 'sysname'
WHERE tt.name = '${row.USER_DEFINED_TYPE_NAME}'
order by c.column_id`, (err, result) => {
                /* $lab:coverage:off$ */
                if (err) {
                  return reject(err);
                }
                /* $lab:coverage:on$ */
                const coltypes = [];
                for (const row of result[0]) {
                  if (dataUserCall.hasOwnProperty(row.colType)) {
                    const callArgs = dataUserCall[row.colType].map((col) => {
                      return row[col];
                    });
                    coltypes.push([row.colName, dataTypes[row.colType].apply(mssql, callArgs)]);
                  } else {
                    coltypes.push([row.colName, dataTypes[row.colType]]);
                  }
                }
                resolve([row.PARAMETER_NAME.slice(1), TVP(coltypes)]);
              });
            })
          );
        }
      }
      return Promise.all(pms)
      .then((args) => {
        return Promise.resolve(args);
      });
    })
    .then((inputs) => {
      const util = require('util');
      opts.args = inputs;
      this[opts.name] = function (obj, args) {
        args = args || {};
        const pm = this.validateAndProcess(obj, 'toDB');
        let db;
        return this.getDB()
        .then((conn) => {
          db = conn;
          return pm;
        }).then((obj) => {
          lodash.assign(obj, args);
          const request = this._makeRequest(db, obj, opts);
          return new Promise((resolve, reject) => {
            request.execute(opts.name, (err, recordsets, returnValue) => {
              /* $lab:coverage:off$ */
              if (err) {
                return reject(err);
              }
              /* $lab:coverage:on$ */
              return resolve(this._procedureResults(opts, recordsets, returnValue));
            });
          });
        });
      }
    });
  };

  _procedureResults(opts, recordsets, returnValue) {
    if (opts.oneResult) {
      if (recordsets[0].length === 0) {
        return Promise.reject(new EmptyResult);
      } else {
        recordsets[0] = recordsets[0].splice(0, 1);
      }
    }

    if (recordsets.length > 1 && recordsets.length !== opts.resultModels.length) {
      return Promise.reject(new Error(`Number of results sets is not equal to the number of resultModels for ${opts.name}`));
    }

    const results = new Map();
    const pms = [];
    for (let idx in recordsets) {
      const rs = recordsets[idx];
      if (rs === 0) break;
      let model = this.getModel(opts.resultModels[idx] || this);
      results.set(model, []);
      rs.forEach((row) => {
        pms.push(
          model.validateAndProcess(row, 'fromDB')
          .then((rowvp) => {
            results.get(model).push(rowvp);
          })
        )
      });
    }
    return Promise.all(pms)
    .then(() => {
      for (let factory of results.keys()) {
        for (let field of Object.keys(factory.map)) {
          let lfield = this.map[field].local;
          let rfield = this.map[field].remote;
          let relFactory = this.getModel(this.map[field].collection || this.map[field].model);
          (results.get(factory)).forEach( (local) => {
            (results.get(relFactory)).forEach( (remote) => {
              if (remote[rfield] == local[lfield]) {
                if (this.map[field].collection) {
                  if (!local[field]) {
                    local[field] = [];
                  }
                  local[field].push(remote);
                } else {
                  local[field] = remote;
                }
              }
            });
          });
        }
      }
      const primaryModel = this.getModel(opts.resultModels[0] || this);
      if (opts.oneResult) {
        return Promise.resolve(results.get(primaryModel)[0]);
      }
      return Promise.resolve(results.get(primaryModel));
    });
  }
}

module.exports = (mssql_config) => {

  let mssql_conn;
  let db_connecting = false;
  let connected_list = [];

  getDB = () => {
    if (!mssql_conn)  {
      if (db_connecting) {
        return new Promise((resolve, reject) => {
          connected_list.push((db) => {
            resolve(db);
          });
        });
      } else {
        db_connecting = true;
        return new Promise((resolve, reject) => {
          const conn = new mssql.Connection(mssql_config, (err) => {
            if (err) return reject(err);
            mssql_conn = conn;
            while (connected_list.length > 0) {
              const cb = connected_list.pop();
              cb(mssql_conn);
            }
            db_connecting = false;
            return resolve(mssql_conn);
          });
        });
      }
    } else {
      return Promise.resolve(mssql_conn);
    }
  };

  return {
    Model,
    getDB,
    EmptyResult,
    getModel: function getModel(name) {
      return cached_models[name];
    },
    TVP,
    Mssql: mssql
  };
};

