'use strict';

const lab = exports.lab = require('lab').script();
const expect = require('code').expect;
const mssql = require('mssql');
const Joi = require('joi');


const config = require('getconfig');

process.on('uncaughtException', function (err) {
  console.log(err.stack);
});

const SQLMoses = require('../index')(config.mssql);

const Test = new SQLMoses.Model({
  name: 'Test',
  schema: Joi.object()
    .rename('FIRST_NAME', 'FirstName', {ignoreUndefined: true})
    .rename('LAST_NAME', 'LastName', {ignoreUndefined: true}),
  }
);

const mp1 = Test.mapProcedure({
  static: true,
  name: 'testproc',
  oneResult: true,
});


const mp2 = Test.mapProcedure({
  static: false,
  oneResult: true,
  name: 'testproc',
});

const p1 = Test.mapStatement({
  static: true,
  oneResult: true,
  name: 'teststate',
  args: [
    ['FirstName', mssql.NVarChar(255)],
    ['LastName', mssql.NVarChar(255)]
  ],
  query: (args) => `SELECT @FirstName AS FirstName, @LastName AS LastName, 'derp' AS hurr`
});

const p2 = Test.mapStatement({
  static: false,
  oneResult: true,
  name: 'teststate',
  args: [
    ['FirstName', mssql.NVarChar(255)],
    ['LastName', mssql.NVarChar(255)]
  ],
  query: (args) => `SELECT @FirstName AS FirstName, @LastName AS LastName, 'derp' AS hurr`
});

const mp3 = Test.mapProcedure({
  static: true,
  name: 'multiselect',
  oneResult: false,
});

const mp4 = Test.mapProcedure({
  static: true,
  name: 'getuno',
  oneResult: true,
});


const p3 = Test.mapStatement({
  static: false,
  name: 'noargs',
  oneResult: true,
  query: (args) => `SELECT 'Billy' AS FirstName, 'Bob' AS LastName`
});

const p4 = Test.mapStatement({
  static: true,
  oneResult: true,
  name: 'test2fail',
  args: [
    ['FirstName', mssql.NVarChar(255)],
    ['LastName', mssql.NVarChar(255)],
    ['Age', mssql.Int]
  ],
  query: (args) => `SELECT @FirstName AS FirstName, @LastName AS LastName, @Age As Age`
});

const mp5 = Test.mapProcedure({
  static: true,
  name: 'customtype',
});

Test.mapQuery({
  static: true,
  oneResult: true,
  name: 'staticquery',
  query: (args) => `SELECT '${args.FirstName}' AS FirstName, '${args.LastName}' AS LastName, 'derp' AS hurr`
});

Test.mapQuery({
  static: false,
  oneResult: true,
  name: 'instancequery',
  query: (args, model) => `SELECT '${args.FirstName}' AS FirstName, '${args.LastName}' AS LastName, 'derp' AS hurr`
});

Test.mapQuery({
  static: true,
  oneResult: true,
  name: 'badquery',
  query: (args, model) => `SELECT PJN ON '${model.FirstName}' AS FirstName, '${model.LastName}' AS LastName, 'derp' AS hurr`
});

Test.mapQuery({
  static: true,
  oneResult: false,
  name: 'morethanone',
  query: (args, model) => `select * from (values ('test-a1', 'test-a2'), ('test-b1', 'test-b2'), ('test-c1', 'test-c2')) x(FirstName, LastName)`
});

Test.mapQuery({
  static: true,
  oneResult: true,
  name: 'getnone',
  query: (args, model) => `select * from sys.types WHERE name='haaaaaybuddy'`
});

lab.experiment('testing functions', () => {
  
  lab.test('create temp table proc', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'multiselect') AND type IN (N'P', N'PC')) DROP PROCEDURE multiselect`).then(() => {
        return request.query(`CREATE PROCEDURE multiselect
AS
CREATE TABLE #TempTest (FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50));
INSERT INTO #TempTest (FIRST_NAME, LAST_NAME) VALUES ('Nathan', 'Fritz'), ('Robert', 'Robles'), ('Cow', 'Town');
SELECT * FROM #TempTest;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });

  let Table;

  lab.test('create table funcs', (done) => {
    Table = new SQLMoses.Model();
    let request;
  
    Test.getDB()
    .then((db) => {
      request = new mssql.Request(db);
      request.multiple = true;
      return request.query(`if exists (select * from sysobjects where name='TempTest2' and xtype='U')
      DROP TABLE TempTest2`);
    }).then(() => {
      return request.query(`CREATE TABLE TempTest2 (id BigInt, FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50));`);
    })
    .then(() => {
      return Table.setTable('TempTest2').then(() => {
        done();
      });
    }).catch(done);
  });


  lab.test('use table funcs', {timeout: 6000}, (done) => {
    let request;
    Test.getDB()
    .then((db) => {
      request = new mssql.Request(db);
      request.multiple = true;
      return Table.insert({FIRST_NAME: 'Nathan', LAST_NAME: 'Fritz'}, 'LAST_NAME')
    })
    .then((results) => {
      expect(results[0].LAST_NAME).to.equal('Fritz');
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
      return Table.select({}, {offset: 0, limit:1, orderBy: 'LAST_NAME'});
    })
    .then((results) => {
      expect(results[0].LAST_NAME).to.equal('Fritz');
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
      done();
      expect(results.length).to.equal(0);
    }).catch(done);
  });
  
  lab.test('write a bad query', (done) => {
    Table.select({'\'derping': 1})
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      //expect(err.message).to.equal('No columns found for view derping');
      done();
    });
  });
  
  
  lab.test('load a non-existant model', (done) => {
    expect(() => {
      Table.getModel('nothere');
    }).to.throw();
    done();
  });

  lab.test('use view funcs', {timeout: 3000}, (done) => {
    let request;
    const TestView = new SQLMoses.Model();
    Test.getDB()
    .then((db) => {
      request = new mssql.Request(db);
      request.query('CREATE VIEW TestView AS SELECT id, LAST_NAME as lastName, FIRST_NAME as firstName FROM TempTest2');
    }).then(() => {
      return TestView.setView('TestView')
    })
    .then(() => {
      return TestView.select();
    })
    .then((results) => {
      expect(results[0].firstName).to.equal('Leo');
        return request.query('DROP TABLE TempTest2; DROP VIEW TestView;', (err, results) => {
          done();
        });
    }).catch(done);
  });

  lab.test('set a non-existant view', (done) => {
    const TestView = new SQLMoses.Model();
    TestView.setView('derping')
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      done();
    });
  });

  lab.test('set a breaking view', (done) => {
    const TestView = new SQLMoses.Model();
    TestView.setView('der\'ping')
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      //expect(err.message).to.equal('No columns found for view derping');
      done();
    });
  });
  
  lab.test('set a breaking table', (done) => {
    const TestTable = new SQLMoses.Model();
    TestTable.setTable('der\'ping')
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      //expect(err.message).to.equal('No columns found for view derping');
      done();
    });
  });
  
  lab.test('set a non existant table', (done) => {
    const TestTable = new SQLMoses.Model();
    TestTable.setTable('derping')
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      done();
    });
  });
  
  lab.test('create user defined table proc', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'customtype') AND type IN (N'P', N'PC')) DROP PROCEDURE customtype`).then(() => {
      }).then(() => {
        return request.query(`IF EXISTS (SELECT * FROM sys.types WHERE is_user_defined=1 AND name='TestUserType') DROP TYPE TestUserType`);
      }).then(() => {
        return request.query(`CREATE TYPE TestUserType AS TABLE (id BigInt, a NVarChar(50), b NVarChar(50))`);
      }).then(() => {
        return request.query(`CREATE PROCEDURE customtype
@id BigInt,
@SomeSub TestUserType READONLY
AS
SELECT a AS FIRST_NAME, b AS LAST_NAME FROM @SomeSub;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });
  
  lab.test('create get table proc', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'getuno') AND type IN (N'P', N'PC')) DROP PROCEDURE getuno`).then(() => {
        return request.query(`CREATE PROCEDURE getuno
    @LAST_NAME VARCHAR(50)
  AS
    CREATE TABLE #TempTest (FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50));
    INSERT INTO #TempTest (FIRST_NAME, LAST_NAME) VALUES ('Nathan', 'Fritz'), ('Robert', 'Robles'), ('Cow', 'Town');
    SELECT * FROM #TempTest WHERE LAST_NAME=@LAST_NAME;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });
  
  lab.test('create select from value', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'getselect') AND type IN (N'P', N'PC')) DROP PROCEDURE getselect`).then(() => {
        return request.query(`CREATE PROCEDURE getselect
    @firstName VARCHAR(50)
  AS
    SELECT @firstName AS firstName;
    SELECT @firstName AS firstName;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });
  
  lab.test('create get table proc2', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'getuno2') AND type IN (N'P', N'PC')) DROP PROCEDURE getuno2`).then(() => {
        return request.query(`CREATE PROCEDURE getuno2
    @LAST_NAME VARCHAR(50)
  AS
    CREATE TABLE #TempTest (FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50));
    INSERT INTO #TempTest (FIRST_NAME, LAST_NAME) VALUES ('Nathan', 'Fritz'), ('Robert', 'Robles'), ('Cow', 'Town');
    SELECT * FROM #TempTest WHERE LAST_NAME=@LAST_NAME;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });
  
  lab.test('create multiresults proc', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'multiresult') AND type IN (N'P', N'PC')) DROP PROCEDURE multiresult`).then(() => {
      return request.query(`CREATE PROCEDURE multiresult
AS
  CREATE TABLE #Name (id INT, first VARCHAR(50), last VARCHAR(50));
  CREATE TABLE #Item (name_id INT, name VARCHAR(50), weight INT);
  INSERT INTO #Name (id, first, last) VALUES (1, 'Nathan', 'Fritz'), (2, 'Robert', 'Robles'), (3, 'Cow', 'Town');
  INSERT INTO #Item (name_id, name, weight) VALUES (1, 'crowbar', 15), (1, 'lettuce', 1), (3, 'cow', 13), (3, 'hair', 0);
  SELECT * FROM #Name;
  SELECT * FROM #Item;`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });
  
  lab.test('create super multiresults proc', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'manyresults') AND type IN (N'P', N'PC')) DROP PROCEDURE manyresults`).then(() => {
      return request.query(`CREATE PROCEDURE manyresults
@inputid BigInt
AS
  SELECT * FROM (VALUES (1, 'Nathan Fritz')) x(id, name);
  SELECT * FROM (VALUES (1, 1, 'Derping for Dummies'), (2, 1, 'Predicting the Future')) x(id, person_id, title);
  SELECT * FROM (VALUES (1, 1, 'Subaru', 'BRZ'), (2, 1, 'Subaru', 'Outback'), (3, 1, 'Dodge', 'Durango')) x(id, person_id, make, model);
  SELECT * FROM (VALUES (1, 1, 'Zips', 'Frier'), (2, 1, 'Watches Watches Watches', 'Clerk')) x(id, person_id, company, title);
  SELECT * FROM (VALUES (1, 1, 'Bert', '02-10-2005')) x(id, person_id, name, dob);
  SELECT * FROM (VALUES (1, 1, 'Fast'), (2, 1, 'Furious'), (3, 1, 'Loud')) x(id, person_id, name);
`);
      }).then(() => {
        done();
      }).catch(done);
    });
  });

  lab.test('loaded statements', (done) => {
    Promise.all([p1, p2, p3, p4, mp1, mp2, mp3, mp4, mp5]).then(() => {
      done();
    }).catch(done);
  });

  lab.test('loading stored procs', (done) => {
    Test.getDB()
    .then((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'testproc') AND type IN (N'P', N'PC')) DROP PROCEDURE testproc;`).then(() => {
        const request2 = new mssql.Request(db);
        return request2.batch(
`
CREATE PROCEDURE testproc
  @FirstName nvarchar(50),
  @LastName nvarchar(50)
AS
  
SELECT @LastName AS LastName, @FirstName AS FirstName;`)
  }).then(() => {
        done();
      }).catch(done);
    });
  });

  lab.test('map many result sets', (done) => {
    const Person = new SQLMoses.Model({
      map: {
        books: {collection: 'Book', local: 'id', remote: 'person_id'},
        cars: {collection: 'Car', local: 'id', remote: 'person_id'},
        jobs: {collection: 'Job', local: 'id', remote: 'person_id'},
        children: {collection: 'Child', local: 'id', remote: 'person_id'},
        traits: {collection: 'Trait', local: 'id', remote: 'person_id'},
      },
      name: 'Person',
    });
    const Book = new SQLMoses.Model({
      name: 'Book',
    });
    const Car = new SQLMoses.Model({
      name: 'Car',
    });
    const Job = new SQLMoses.Model({
      name: 'Job',
    });
    const Child = new SQLMoses.Model({
      name: 'Child',
    });
    const Trait = new SQLMoses.Model({
      name: 'Trait',
    });
    Person.mapProcedure({
      static: true,
      name: 'manyresults',
      oneResult: true,
      resultModels: ['Person', 'Book', 'Car', 'Job', 'Child', 'Trait']
    })
    .then(() => {
      Person.manyresults({inputid: 7}).then((person) => {
        expect(person.jobs[1].company).to.equal('Watches Watches Watches');
        expect(person.children[0].name).to.equal('Bert');
        expect(person.cars[2].make).to.equal('Dodge');
        expect(person.traits.length).to.equal(3);
        done();
      }).catch(done);
    });
  });
   

  lab.test('bad mapping of result sets', (done) => {
    const Person = SQLMoses.getModel('Person');
    Person.mapProcedure({
      static: true,
      name: 'manyresults',
      oneResult: true,
      resultModels: ['Person', 'Car', 'Job', 'Child', 'Trait']
    })
    .then(() => {
      Person.manyresults({inputid: 7}).then((person) => {
        done(new Error('should have thrown'));
      }).catch((e) => {
        done();
      });
    });
  });

  lab.test('static procedure', (done) => {
    Test.testproc({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    });
  });

  lab.test('instance procedure', (done) => {
    const test = {
      FirstName: 'Nathanael',
      LastName: 'Fritzer'
    };
    Test.testproc(test).then((results) => {
      expect(results.FirstName).to.equal('Nathanael');
      expect(results.LastName).to.equal('Fritzer');
      done();
    }).catch(done);
  });

  lab.test('static statement', (done) => {
    Test.teststate({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });

  lab.test('instance statement', (done) => {
    const test = {
      FirstName: 'Nathan',
      LastName: 'Fritz',
    };
    Test.teststate(test).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });
  
  lab.test('static query', (done) => {
    Test.staticquery({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });

  lab.test('instance query', (done) => {
    const test = {
      FirstName: 'Nathan',
      LastName: 'Fritz',
    };
    Test.instancequery(test).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });
  
  lab.test('multi static statement', (done) => {
    Test.multiselect().then((results) => {
      expect(results[0].FirstName).to.equal('Nathan');
      expect(results[0].LastName).to.equal('Fritz');
      expect(results.length).to.equal(3);
      done();
    }).catch(done);
  });
  
  lab.test('unfound static staetment', (done) => {
    Test.getuno({LAST_NAME: 'Derpy'}).then((results) => {
      done('should have errored');
    }).catch((err) => {
      expect(err).to.be.an.instanceof(SQLMoses.EmptyResult);
      done();
    });
  });
  
  lab.test('single static statement', (done) => {
    Test.getuno({}, {LAST_NAME: 'Fritz'}).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });

  lab.test('join multi collection', (done) => {
    const Item = new SQLMoses.Model({ name: 'jm_item' });
    const Name = new SQLMoses.Model({
      map: {
        items: { collection: 'jm_item', local: 'id', 'remote': 'name_id' }
      },
      name: 'rm_name'});
    Name.mapProcedure({
      static: true,
      name: 'multiresult',
      oneResult: false,
      resultModels: ['', 'jm_item']
    })
    .then(() => {
      Name.multiresult()
      .then((names) => {
        expect(names[0].items[1].name).to.equal('lettuce');
        expect(names[2].items[1].name).to.equal('hair');
        done();
      }).catch(done);
    });
  });

  lab.test('join multi model', (done) => {
    const Item = new SQLMoses.Model({name: 'jm_item2'});
    const Name = new SQLMoses.Model({
      map: {
        'item': { model: 'jm_item2', local: 'id', 'remote': 'name_id' }
      },
      name: 'rm_name2'
    });
    Name.mapProcedure({
      static: true,
      name: 'multiresult',
      oneResult: false,
      resultModels: ['', 'jm_item2']
    })
    .then(() => {
      Name.multiresult()
      .then((names) => {
        expect(names[0].item.name).to.equal('lettuce');
        expect(names[2].item.name).to.equal('hair');
        done();
      }).catch(done);
    });
  });


  lab.test('get model', (done) => {
    const model = SQLMoses.getModel('Test');
    done();
  });

  lab.test('bad MapQuery', (done) => {
    expect(() => {
      Test.mapQuery({
      });
    }).to.throw();
    done();
  });
  
  lab.test('bad mapStatement', (done) => {
    expect(() => {
      Test.mapStatement({
      });
    }).to.throw();
    done();
  });
  
  lab.test('bad mapProcedure', (done) => {
    expect(() => {
      Test.mapProcedure({
      });
    }).to.throw();
    done();
  });

  lab.test('bad query', (done) => {
    expect(() => {
      Test.badQuery({
      });
    }).to.throw();
    done();
  });

  /*
  lab.test('queryResults error', (done) => {
    Test._queryResults({}, 'ERROR', [], 0, Promise.resolve, function () {
      done();
    });
  });
  */


  /*
  lab.test('processArgs static', (done) => {
    Test.mapProcedure({
      static: true,
      name: 'getuno2',
      args: [
        ['LAST_NAME', mssql.NVarChar(50)]
      ],
      processArgs: function (args, model) {
        if (args.last) {
          args.LAST_NAME = args.last;
          delete args.last;
        }
        return args;
      },
      oneResult: true,
    });
    Test.getuno2({last: 'Fritz'}).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });

  lab.test('processArgs instance', (done) => {
    Test.mapProcedure({
      static: false,
      name: 'getuno2',
      args: [
        ['LAST_NAME', mssql.NVarChar(50)]
      ],
      processArgs: function (args, model) {
        if (args.last) {
          args.LAST_NAME = args.last;
          delete args.last;
        }
        return args;
      },
      oneResult: true,
    });
    let test = Test.create({});
    test.getuno2({last: 'Fritz'}).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });
  */

  lab.test('no args statement', (done) => {
    const test = {};
    Test.noargs()
    .then((model) => {
      expect(model.FirstName).to.equal('Billy');
      expect(model.LastName).to.equal('Bob');
      done();
    }).catch(done);
  });
  
  lab.test('custom type static', (done) => {
    Test.customtype({id: 1, SomeSub: [{id: 1, a: 'Billy', b: 'Bob',}, {id: 2, a: 'Ham', b: 'Sammich'}]}).then((model) => {
      expect(model[0].FirstName).to.equal('Billy');
      expect(model[0].LastName).to.equal('Bob');
      expect(model[1].FirstName).to.equal('Ham');
      expect(model[1].LastName).to.equal('Sammich');
      done();
    }).catch(done);
  });

  lab.test('more than one result from query', (done) => {
    Test.morethanone().then((model) => {
      expect(model[0].FirstName).to.equal('test-a1');
      expect(model[1].FirstName).to.equal('test-b1');
      done();
    }).catch(done);
  });
  
  lab.test('get no results on oneResult:true', (done) => {
    Test.getnone().then((model) => {
      done('Should Have Errored');
    }).catch((err) => {
      expect(err).to.be.an.instanceof(SQLMoses.EmptyResult);
      done();
    });
  });
  
  lab.test('custom type method', (done) => {
    const HasSubType = new SQLMoses.Model({
      id: {},
      SomeSub: {collection: 'UserType'}
    });
    const UserType = new SQLMoses.Model({
      a: {},
      b: {}
    }, {
      name: 'UserType',
      cache: true
    });

    HasSubType.mapProcedure({
      static: false,
      name: 'customtype',
      resultModels: ['Test']
    })
    .then(() => {
      const model = {id: 1, SomeSub: [{a: 'Billy', b: 'Bob'}, {a: 'Ham', b: 'Sammich'}]};
      HasSubType.customtype(model).then((rmodel) => {
        expect(rmodel[0].FirstName).to.equal('Billy');
        expect(rmodel[0].LastName).to.equal('Bob');
        expect(rmodel[1].FirstName).to.equal('Ham');
        expect(rmodel[1].LastName).to.equal('Sammich');
        done();
      }).catch(done);
    });
  });

  lab.test('bad prepared statement', (done) => {
    Test.mapStatement({
      static: false,
      name: 'noargs',
      oneResult: true,
      query: (args) => `SERECT 'Billy' AS FirstName, 'Bob' AS LastName`
    }).then(() => {
      done(new Error('there should have been an error'));
    }).catch((err) => {
      done();
    });
  });

  lab.test('bad arguments in statement', (done) => {
    Test.test2fail({FirstName: 'Nathan', LastName: 'Fritz', Age: 'Thirty-Four'}).then(() => {
      done(new Error('should error out'));
    }).catch((error) => {
      done();
    });
  });

  lab.test('inDB outDB processor', (done) => {
      const Model = new SQLMoses.Model({
        processors: {
          fromDB: {
            firstName: function (value) {
                return value.replace('-', ' ');
            }
          },
          toDB: {
            firstName: function (value) {
                return value.replace(' ', '_') + '-x';
            }
          }
        }
      });
    Model.mapQuery({
      static: true,
      oneResult: false,
      name: 'testOut',
      query: (args, model) => `select * from (values ('test-a1'), ('test-b1'), ('test-c1')) x(firstName)`
    });
    Model.mapProcedure({
      static: false,
      oneResult: false,
      name: 'getselect',
      resultModels: [Model, Model]
    })
    .then(() => {
      Model.testOut().then((models) => {
        expect(models[0].firstName).to.equal('test a1');
        expect(models[1].firstName).to.equal('test b1');
        expect(models[2].firstName).to.equal('test c1');
        return Model.getselect(models[0]);
      }).then((model) => {
        expect(model[0].firstName).to.equal('test_a1 x');
        done();
      }).catch(done);
    });
  });

  lab.test('null TVP', (done) => {
    Test.customtype({id: 1}).then((model) => {
      expect(Array.isArray(model)).to.equal(true);
      expect(model.length).to.equal(0);
      done();
    }).catch(done);
  });
  
  lab.test('connect to bad db', (done) => {
    const conn = require('../index')({host: 'ham samdfmka'});
    conn.getDB()
    .then(() => {
      done(new Error('this shouldn\'t happen'));
    })
    .catch((err) => {
      done();
    });
  });
  
  lab.test('disconnect', (done) => {
    Test.getDB()
    .then((db) => {
      Test.unprepare('teststate').then(() => {
        db.close();
        done();
      });
    });
  });
});
