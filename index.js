"use strict";

const verymodel = require('verymodel');
const lodash = require('lodash');
const assert = require('assert');
const Joi = require('joi');
const mssql = require('mssql');
const _ = require('underscore');

const methodValidators = {
  mapProcedure: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    args: Joi.array(Joi.array().length(2)),
    output: Joi.array().min(2).max(2),
    oneResult: Joi.boolean().default(false),
    processArgs: Joi.func(),
    resultModels: Joi.array(Joi.string())
  }),
  mapStatement: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    args: Joi.array(Joi.array().length(2)),
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

function EmptyResult() {
  Error.apply(this, arguments);
  this.message = "EmptyResult";
}

EmptyResult.prototype = Object.create(Error.prototype);

let connection = null;

const cached_models = {};

function Model() {
  verymodel.VeryModel.apply(this, arguments);

  if (this.options.name) {
    cached_models[this.options.name] = this;
  }

  this._preparedStatements = {};

  /* $lab:coverage:off$ */
  if (connection !== null) {
    this.options.mssql = connection;
  }
  /* $lab:coverage:on$ */

  this.getDB = (cb) => {
    if (this.mssql === null)  {
      const conn = new mssql.Connection(this.options.mssql, (err) => {
        /* $lab:coverage:off$ */
        if (err) throw err;
        /* $lab:coverage:on$ */
        this.mssql = conn;
        cb(conn);
      });
    } else {
      cb(this.mssql);
    }
  };

  this.unprepare = function unprepare(name) {
    return this._preparedStatements[name].unprepare();
  };

  this.mssql = null;
}

Model.prototype = Object.create(verymodel.VeryModel.prototype);

(function () {

  this.mapQuery = function mapStatement(opts) {
    const optsValid = methodValidators.mapQuery.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    if (opts.static) {
      return this._mapStaticQuery(opts);
    } else {
      return this._mapInstanceQuery(opts);
    }
  };

  this._mapStaticQuery = function mapStaticQuery(opts) {
    Model.prototype[opts.name] = function (args) {
      const promise = new Promise((resolve, reject) => {
        this.getDB((db) => {
          const query = new mssql.Request(db);
          const qstring = opts.query(args);
          query.query(qstring, (err, recordset) => {
            return this._queryResults(opts, err, recordset, undefined, resolve, reject);
          });
        });
      });
      return promise;
    }
  };

  this._mapInstanceQuery = function mapInstanceQuery(opts) {
    const extension = {};
    const model = this;
    extension[opts.name] = function (args) {
      const promise = new Promise((resolve, reject) => {
        model.getDB((db) => {
          const request = new mssql.Request(db);
          const qstring = opts.query(args, this);
          request.query(qstring, (err, recordset) => {
            return model._queryResults(opts, err, recordset, undefined, resolve, reject);
          });
        });
      });
      return promise;
    }
    this.extendModel(extension);
  };

  this.mapStatement = function mapStatement(opts) {
    const optsValid = methodValidators.mapStatement.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    opts.args = opts.args || [];
    if (opts.static) {
      return this._mapStaticStatement(opts);
    } else {
      return this._mapInstanceStatement(opts);
    }
  };

  this._mapStaticStatement = function mapStaticStatement(opts) {
    Model.prototype[opts.name] = function (args) {
      const promise = new Promise((resolve, reject) => {
        this._preparedStatements[opts.name].execute(args, (err, recordset, returnValue) => {
          return this._queryResults(opts, err, recordset, returnValue, resolve, reject);
        });
      });
      return promise;
    }
    return this._prepare(opts);
  };

  this._mapInstanceStatement = function mapInstanceStatement(opts) {
    const extension = {};
    const model = this;
    extension[opts.name] = function (args) {
      const input = this.toJSON();
      lodash.assign(input, args);
      const promise = new Promise((resolve, reject) => {
        model._preparedStatements[opts.name].execute(input, (err, recordset, returnValue) => {
          return model._queryResults(opts, err, recordset, returnValue, resolve, reject);
        });
      });
      return promise;
    };
    this.extendModel(extension);
    return this._prepare(opts);
  };

  this._queryResults = function _queryResults(opts, err, recordset, returnValue, resolve, reject) {
    if (err) {
      return reject(err);
    }
    if (opts.oneResult) {
      if (recordset.length === 0 || recordset[0].length === 0) {
        return reject(new EmptyResult);
      } else {
        return resolve(this.create(recordset[0]));
      }
    }
    const results = [];
    recordset.forEach((row) => {
      results.push(this.create(row));
    });
    return resolve(results);
  }

  this._prepare = function _prepare(opts) {
    return new Promise((resolve, reject) => {
      this.getDB((db) => {
        const statement = new mssql.PreparedStatement(db);
        opts.args.forEach((arg) => {
          statement.input(arg[0], arg[1]);
        });
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

  this.mapProcedure = function mapProcedure(opts) {
    const optsValid = methodValidators.mapProcedure.validate(opts);
    if (optsValid.error) {
      throw optsValid.error;
    }
    opts.args = opts.args || [];
    opts.resultModels = opts.resultModels || [this];
    if (opts.static) {
      return this.mapStaticProc(opts);
    } else {
      return this.mapInstProc(opts);
    }
  };

  this._makeRequest = function (db, args, opts) {
    const request = new mssql.Request(db);
    opts.args.forEach((arg) => {
      const argdef = arg[1];
      const field = arg[0];
      if (argdef.hasOwnProperty('type') && argdef.type === 'TVP' && args.hasOwnProperty(field)) {
        let tvp = new mssql.Table();
        argdef.types.forEach((mtype) => {
          tvp.columns.add(mtype[0], mtype[1]);
        });
        args[field].forEach((argrow) => {
          let row = [];
          argdef.types.forEach((mtype) => {
            let col = mtype[0];
            row.push(argrow[col]);
          });
          tvp.rows.add.apply(tvp.rows, row);
        });
        request.input(field, tvp);
      } else {
        request.input(field, argdef, args[field]);
      }
    });
    return request;
  };

  this.mapStaticProc = function mapStaticProc(opts) {
    Model.prototype[opts.name] = function (args) {
      args = args || {};
      if (typeof opts.processArgs === 'function') {
        args = opts.processArgs(args, this);
      }
      const promise = new Promise((resolve, reject) => {
        this.getDB((db) => {
          const request = this._makeRequest(db, args, opts);
          request.execute(opts.name, (err, recordsets, returnValue) => {
            return this._procedureResults(opts, err, recordsets, returnValue, reject, resolve);
          });
        });
      });
      return promise;
    }
  };

  this.mapInstProc = function mapInstProc(opts) {
    const extension = {};
    const model = this;
    extension[opts.name] = function (args) {
      args = args || {};
      args = _.extend(this.toJSON(), args);
      if (typeof opts.processArgs === 'function') {
        args = opts.processArgs(args, this);
      }
      const promise = new Promise((resolve, reject) => {
        model.getDB((db) => {
          const request = model._makeRequest(db, args, opts);
          request.execute(opts.name, (err, recordsets, returnValue) => {
            return model._procedureResults(opts, err, recordsets, returnValue, reject, resolve);
          });
        });
      });
      return promise;
    };
    this.extendModel(extension);
  };

  this._procedureResults = function _procedureResults(opts, err, recordsets, returnValue, reject, resolve) {
    //sending bad data to a procedure doesn't throw an error
    //so I'm unsure how to make an error happen here
    /* $lab:coverage:off$ */
    if (err) {
      return reject(err);
    }
    /* $lab:coverage:on$ */
    if (opts.oneResult) {
      if (recordsets[0].length === 0) {
        return reject(new EmptyResult);
      } else {
        recordsets[0] = recordsets[0].splice(0, 1);
      }
    }
    const results = new Map();
    for (let idx in recordsets) {
      const rs = recordsets[idx];
      if (rs === 0) break;
      let model = opts.resultModels[idx] || this;
      if (typeof model === 'string') {
        model = this.getModel(model);
      }
      results.set(model, []);
      rs.forEach((row) => {
        results.get(model).push(model.create(row));
      });
    }
    for (let factory of results.keys()) {
      for (let field of factory.fields) {
        if (factory.definition[field].hasOwnProperty('remote')) {
          let lfield = factory.definition[field].local;
          let rfield = factory.definition[field].remote;
          let relFactory = this.getModel(factory.definition[field].collection || factory.definition[field].model);
          (results.get(factory)).forEach( (local) => {
            (results.get(relFactory)).forEach( (remote) => {
              if (remote[rfield] == local[lfield]) {
                local[field] = remote;
              }
            });
          });
        }
      }
    }
    let primaryModel = opts.resultModels[0] || this;
    if (typeof primaryModel === 'string') {
      primaryModel = this.getModel(primaryModel);
    }
    if (opts.oneResult) {
      return resolve(results.get(primaryModel)[0]);
    }
    return resolve(results.get(primaryModel));
  }

}).call(Model.prototype);

module.exports = {
  Model,
  setConnection: function setConnection(opts) {
    connection = opts;
  },
  EmptyResult,
  getModel: function getModel(name) {
    return cached_models[name];
  },
  TVP: function TVP(types) {
    return {type: 'TVP', types};
  }
};
