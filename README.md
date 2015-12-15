# Seaquell

A reverse-ORM for query Microsoft SQL Server.

In a reverse-ORM, you model what the query results in rather than modeling the tables.

Models are extensions of [VeryModel](https://github.com/fritzy/verymodel).

## Example

```javascript
'use strict';

const Seaquell = require('seaquell');
const mssql = require('mssql');

Sealquell.setConnection({
  "user": "sa",
  "password": "password",
  "server": "localhost",
  "database": "seaquell_test",
  "pool": {
    "min": 3,
    "max": 10
  }
});

const Test = new Seaquell.Model({
  FirstName: {},
  LastName: {},
});

Test.mapProcedure({
  static: true,
  name: 'testproc',
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  }
});

Test.mapProcedure({
  static: false,
  name: 'testproc',
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  }
});

Test.testproc({
  FirstName: 'Nathan',
  LastName: 'Fritz',
}).then((results) => {
  console.log(results.results[0].toJSON());
    // {FirstName: "Nathan", LastName: "Fritz"}
  const test = results.results[0];
  test.FirstName = 'Nathanael';
  return test.testproc();
}).then((response) => {
  console.log(response.results[0].toString());
  // {"FirstName": "Nathanael", "LastName": "Fritz"}
  Test.getDB((db) => {
    db.close();
  });
}).catch((err) => {
  console.log(err.stack);
});
```

## Install

![npm i seaquell](https://nodei.co/npm/seaquell.png)

## setConnection

Call `Seaquell.setConnection({})` with the same options you'd use for [node-mssql](https://github.com/patriksimek/node-mssql#basic-configuration-is-same-for-all-drivers).

## Creating a Model 

Instantiate a new model with `new Seaquell.Model`. See [VeryModel](https://github.com/fritzy/verymodel) for examples.

You can set the model option of `mssql` for individual models to have different connetions.

## Methods

### mapStatement

Creates a method that runs a Prepared Statement, returning a Promise with model instances of the resulting rows.

__mapStatement__(opts)

```
opts: {
  name: (String) name of method,
  args: { //input parameters for the prepared statement
    String() name of parameter: mssql.type() a valid mssql type,
    etc..
  },
  output: { //output parameters for the prepared statement
    String() name of parameter: mssql.type() a valid mssql type,
    etc..
  },
  oneResult: (Boolean) return array of model instance if false (default) or single if true,
  static: (Boolean) attach to Model Factory or model instances
}
```

__return__: Promise that waits for the prepared statement to be setup.

__Note:__: When the method is attached to a model instance (static: false), the model instance fields are used as default values for the query.

####Usage

ModelName.optsName(args) or Instance.optName(args)

__return__: Promise with `{results, output}` or `modelInstance` if `oneResult` set to `true`.

### mapProcedure

Creates a method that runs a Stored Procedure, returning a Promise with model instances of the resulting rows.

__mapProcedure__(opts)

```
opts: {
  name: (String) name of method,
  args: { //input parameters for the prepared statement
    String() name of parameter: mssql.type() a valid mssql type,
    etc..
  },
  output: { //output parameters for the prepared statement
    String() name of parameter: mssql.type() a valid mssql type,
    etc..
  },
  oneResult: (Boolean) return array of model instance if false (default) or single if true,
  static: (Boolean) attach to Model Factory or model instances,
  resultModels: (Array) string names of Model Factories to use for creating recordsets if more than one
  processArgs: (function(args, model) return args) function to process incoming args of resulting method before passing it on to the stored proceedure. The 2nd arguement will be the factory for static methods and the model instance for non-static methods.
}
```

__return__: `undefined`

__Note:__: When the method is attached to a model instance (static: false), the model instance fields are used as default values for the query.

####Usage

ModelName.optsName(args) or Instance.optName(args)

__return__: Promise with `{results, output}` or `modelInstance` if `oneResult` set to `true`.

### mapQuery

Create a method that runs a raw query, returning a Promise with model instances of the resulting rows.

__mapQuery__(opts)

```
opts: {
  name: (String) name of method,
  query: (function) function returning query string. passed (args, instance)
  oneResult: (Boolean) return array of model instance if false (default) or single if true,
  static: (Boolean) attach to Model Factory or model instances
}
```

__return__: `undefined`

# Seaquell.getModel(name)

__seaquell.getModel(name)__

Returns the model named 'name';
