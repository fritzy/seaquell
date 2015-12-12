"use strict";

const verymodel = require('verymodel');
const lodash = require('lodash');
const assert = require('assert');
const Joi = require('joi');
const mssql = require('mssql');

const methodValidators = {
  mapProcedure: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    args: Joi.object().unknown(),
    output: Joi.array().min(2).max(2),
    oneResult: Joi.boolean().default(false)
  }),
  mapStatement: Joi.object({
    static: Joi.boolean().default(false),
    name: Joi.string().required(),
    args: Joi.object().unknown(),
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

  if (connection !== null) {
    this.options.mssql = connection;
  }

  this.getDB = (cb) => {
    if (this.mssql === null)  {
      const conn = new mssql.Connection(this.options.mssql, (err) => {
        if (err) throw err;
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
            if (err) {
              return reject(err);
            }
            if (opts.oneResult) {
              if (recordset[0].length === 0) {
                return reject(new EmptyResult);
              } else {
                return resolve(this.create(recordset[0]));
              }
            }
            const results = [];
            recordset[0].forEach((row) => {
              results.push(this.create(row));
            });
            resolve({results, returnValue});
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
            if (err) {
              return reject(err);
            }
            if (opts.oneResult) {
              if (recordset[0].length === 0) {
                return reject(new EmptyResult);
              } else {
                return resolve(model.create(recordset[0]));
              }
            }
            const results = [];
            recordset[0].forEach((row) => {
              results.push(model.create(row));
            });
            resolve({results, returnValue});
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
          if (err) {
            return reject(err);
          }
          if (opts.oneResult) {
            if (recordset[0].length === 0) {
              return reject(new EmptyResult);
            } else {
              return resolve(this.create(recordset[0]));
            }
          }
          const results = [];
          recordset[0].forEach((row) => {
            results.push(this.create(row));
          });
          resolve({results, returnValue});
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
      if (typeof args === 'undefined') {
        args = {};
      }
      const input = this.toJSON();
      lodash.assign(input, args);
      const promise = new Promise((resolve, reject) => {
        model._preparedStatements[opts.name].execute(input, (err, recordset, returnValue) => {
          if (err) {
            return reject(err);
          }
          if (opts.oneResult) {
            if (recordset[0].length === 0) {
              return reject(new EmptyResult);
            } else {
              return resolve(model.create(recordset[0]));
            }
          }
          const results = [];
          recordset.forEach((row) => {
            results.push(model.create(row));
          });
          resolve({results, returnValue});
        });
      });
      return promise;
    };
    this.extendModel(extension);
    return this._prepare(opts);
  };

  this._prepare = function _prepare(opts) {
    return new Promise((resolve, reject) => {
      this.getDB((db) => {
        const statement = new mssql.PreparedStatement(db);
        if (opts.args) {
          Object.keys(opts.args).forEach((arg) => {
            statement.input(arg, opts.args[arg]);
          });
        }
        if (opts.output) {
          Object.keys(opts.output).forEach((arg) => {
            statement.output(arg, opts.args[arg]);
          });
          statement.output(opts.output[0], opts.output[1]);
        }
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
    if (opts.static) {
      return this.mapStaticProc(opts);
    } else {
      return this.mapInstProc(opts);
    }
  };

  this.mapStaticProc = function mapStaticProc(opts) {
    Model.prototype[opts.name] = function (args) {
      const promise = new Promise((resolve, reject) => {
        this.getDB((db) => {
          const request = new mssql.Request(db);
          if (opts.args) {
            Object.keys(opts.args).forEach((arg) => {
              request.input(arg, opts.args[arg], args[arg]);
            });
          }
          if (opts.output) {
            request.output(opts.output[0], opts.output[1]);
          }
          request.execute(opts.name, (err, recordset, returnValue) => {
            if (err) {
              return reject(err);
            }
            if (opts.oneResult) {
              if (recordset[0].length === 0) {
                return reject(new EmptyResult);
              } else {
                return resolve(this.create(recordset[0][0]));
              }
            }
            const results = [];
            recordset[0].forEach((row) => {
              results.push(this.create(row));
            });
            resolve(results);
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
      if (typeof args === 'undefined') {
        args = {};
      }
      const promise = new Promise((resolve, reject) => {
        model.getDB((db) => {
          const request = new mssql.Request(db);
          if (opts.args) {
            Object.keys(opts.args).forEach((arg) => {
              request.input(arg, opts.args[arg], this[arg] || args[arg]);
            });
          }
          if (opts.output) {
            request.output(opts.output[0], opts.output[1]);
          }
          request.execute(opts.name, (err, recordset, returnValue) => {
            if (err) {
              return reject(err);
            }
            if (opts.oneResult) {
              if (recordset[0].length === 0) {
                return reject(new EmptyResult);
              } else {
                return resolve(model.create(recordset[0][0]));
              }
            }
            const results = [];
            recordset[0].forEach((row) => {
              results.push(model.create(row));
            });
            resolve(results);
          });
        });
      });
      return promise;
    };
    this.extendModel(extension);
  };

}).call(Model.prototype);

module.exports = {
  Model,
  setConnection: function setConnection(opts) {
    connection = opts;
  },
  EmptyResult,
  getModel: function getModel(name) {
    return cached_models[name];
  }
};
