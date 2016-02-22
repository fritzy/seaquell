# SQLMoses

![Seagull Waterbender Moses](https://cldup.com/xBEt5glGHQ.png)

[![npm version](https://badge.fury.io/js/sqlmoses.svg)](http://badge.fury.io/js/sqlmoses)

A [pure function](http://www.nicoespeon.com/en/2015/01/pure-functions-javascript/) model class for MS SQL Server.

Methods are passed a normal object and respond with a new object.
Incoming and outgoing objects are validated and transformed by [Joi](https://npmjs.org/package/joi) schemas when supplied, and transformed by processing functions when supplied.

The Model instance itself does not keep track of fields, you are expected to pass in an object to every function.

You can auto-generate table methods (insert, update, delete, select). These methods use safe prepared statements, having auto-detected the table field types.
You can also generate stored procs methods, which deal with the types automatically as well.

You can always bind a raw query or prepared statement of your design as well.

## Example

```javascript
'use strict';

const SQLMoses = require('sqlmoses')({
  "user": "sa",
  "password": "password",
  "server": "localhost",
  "database": "sqlmoses_test",
  "pool": {
    "min": 3,
    "max": 10
  }
});

const Test = new SQLMoses.Model({
  name: 'test',
  schema: Joi.object({
    FirstName: joi.string(),
    LastName: joi.string()
  })
});

Promise.all(Test.mapProcedure({
  static: true,
  name: 'testproc',
}),
Test.mapProcedure({
  static: false,
  name: 'testproc',
})).then(() => 

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
}
```

## Install

[![npm i sqlmoses](https://nodei.co/npm/sqlmoses.png)](https://npmjs.org/packages/sqlmoses)


## Creating a Model 

```js
new SQLMoses.Model({
  name: 'someModel'
  keyMap: {
    'renameKey': 'toThis'
  },
  map: {
    someField: {
      'collection or model': 'otherModelName',
      remote: 'someRemoteId',
      local: 'localId'
    }
  },
  schema: Joi.object(),
  processors: {
    'processorName': {
      fieldName: (input, model) => {
        return Promise.resolve(input+'modification');
      }
    }
  }
})
```

* `map`: has fields with a sub `collection` or sub `model`, `remote` and `local` attributes to indicate how resulting objects should be joined into the parent model. Used in conjunction with `resultModels` to map the resultsets back to the right Model.
* `name`: names the model so that you can reference it by string in map and other places
* `schema`: [Joi](https://npmjs.org/package/joi) schema object. Keep in mind, joi can do transforms (rename, casting, etc)
* `processors` object of processor tags with field transformations. Called when Model`.process` is called.
  * The custom processor `fromDB` is called when models are being created from the db results.
  * The custom processor `toDB` is called when model instances are used as input for stored procs.

## Methods

### mapStatement

Creates a method that runs a Prepared Statement, returning a Promise with model instances of the resulting rows.

__mapStatement__(opts)

```
opts: {
  name: (String) name of method,
  args: [ //input parameters for the prepared statement
    [String() name of parameter, mssql.type a valid mssql type or SQLMoses.TVP()],
    [string, type],
    ...
  ],
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

Model[statementName](modelobj, args)

__return__: Promise with `{results, output}` or `modelInstance` if `oneResult` set to `true`.

### mapProcedure

Creates a method that runs a Stored Procedure, returning a Promise with model instances of the resulting rows.

__mapProcedure__(opts)

```
opts: {
  name: (String) name of method,
  oneResult: (Boolean) return array of model instance if false (default) or single if true,
  resultModels: (Array) string names of Model Factories to use for creating recordsets if more than one
  processArgs: (function(args, model) return args) function to process incoming args of resulting method before passing it on to the stored proceedure. The 2nd arguement will be the factory for static methods and the model instance for non-static methods.
}
```

__return__: Promise awaiting setup.

####Usage

ModelName\[name\](modelobj, args)

__return__: Promise with array of model validated objects or a singular result if `oneResult` set to `true`.

### mapQuery

Create a method that runs a raw query, returning a Promise with model instances of the resulting rows.

__mapQuery__(opts)

```
opts: {
  name: (String) name of method,
  query: (function) function returning query string. passed (args, instance)
  oneResult: (Boolean) return array of model instance if false (default) or single if true,
}
```

__return__: `undefined`

### setTable(name)

Sets up insert(obj), update(obj, whereobj), select(whereobj), delete(whereobj)

Returns a Promise awaiting the configuration of these methods.

```js
Table.insert({FIRST_NAME: 'Nathan', LAST_NAME: 'Fritz'})
.then(() => {
  return Table.select()
})
.then((results) => {
  expect(results[0].FIRST_NAME).to.equal('Nathan');
})
.then(() => {
  return Table.insert({FIRST_NAME: 'Bob', LAST_NAME: 'Sagat'});
})
.then(() => {
  return Table.select({LAST_NAME: 'Sagat'})
})
.then((results) => {
  expect(results[0].FIRST_NAME).to.equal('Bob');
  return Table.update({FIRST_NAME: 'Leo'}, {LAST_NAME: 'Sagat'});
})
.then(() => {
  return Table.select({LAST_NAME: 'Sagat'})
})
.then((results) => {
  expect(results[0].FIRST_NAME).to.equal('Leo');
  return Table.delete({LAST_NAME: 'Fritz'})
})
.then(() => {
  return Table.select({LAST_NAME: 'Fritz'})
})
.then((results) => {
  expect(results.length).to.equal(0);
});
```

### validate(obj)

Validates using the Joi schema resulting in a new (remember that Joi can transform) object from a Promise.

### process(obj, tags)

Runs processing tags against .processors resulting in a new object from a Promise.

## validateAndProcess(obj, tags)

Runs both validation and processors resulting in a new object from a Promise.

### SQLMoses.getModel(name)

__model.getModel(name)__

Returns the model named 'name';

### SQLMoses.TVP(types)

Allows you to use a Table Value Parameter as an input to a Stored Procedure

Similar to "args" in mapProcedure, the types argument is an array of arrays.

####Usage

```js
const Book = new SQLMoses.Model({
});
const Author = new SQLMoses.Model({
  map: {
    books: {collection: 'Book'}
  }
});

Author.mapProcedure({
  args: [
    ['name', mssql.NVarChar(50)],
    ['books', SQLMoses.TVP([
      'title', mssql.NVarChar(50)
    ])]
  ],
  name: 'insertAuthorWithBooks'
})
.then(() => {

  const author = {
    name: 'Nathan Fritz',
    books: [
      {title: 'A Tale of Ham'},
      {title: 'Why Now?'}
    ]
  };

  author.insertAuthorWithBooks().then(() => {
    //tada
  });
});
```

