'use strict';

const lab = exports.lab = require('lab').script();
const expect = require('code').expect;
const mssql = require('mssql');

const Seaquell = require('../index');
const config = require('getconfig');

process.on('uncaughtException', function (err) {
  console.log(err.stack);
});

Seaquell.setConnection(config.mssql);

const Test = new Seaquell.Model({
  FirstName: {alias: 'FIRST_NAME'},
  LastName: {alias: 'LAST_NAME'},
}, {
  name: 'Test',
  cache: true
});

Test.mapProcedure({
  static: true,
  name: 'testproc',
  oneResult: true,
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  }
});


Test.mapProcedure({
  static: false,
  oneResult: true,
  name: 'testproc',
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  }
});

const p1 = Test.mapStatement({
  static: true,
  oneResult: true,
  name: 'teststate',
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  },
  query: (args) => `SELECT @FirstName AS FirstName, @LastName AS LastName, 'derp' AS hurr`
});

const p2 = Test.mapStatement({
  static: false,
  oneResult: true,
  name: 'teststate',
  args: {
    'FirstName': mssql.NVarChar(255),
    'LastName': mssql.NVarChar(255)
  },
  query: (args) => `SELECT @FirstName AS FirstName, @LastName AS LastName, 'derp' AS hurr`
});

Test.mapProcedure({
  static: true,
  name: 'multiselect',
  oneResult: false,
});

Test.mapProcedure({
  static: true,
  name: 'getuno',
  args: {
    'LAST_NAME': mssql.NVarChar(50)
  },
  oneResult: true,
});

const p3 = Test.mapStatement({
  static: false,
  name: 'noargs',
  oneResult: true,
  query: (args) => `SELECT 'Billy' AS FirstName, 'Bob' AS LastName`
});

Test.mapProcedure({
  static: true,
  name: 'customtype',
  args: {
    id: mssql.BigInt,
    SomeSub: Seaquell.TVP([
      ['a', mssql.NVarChar(50)],
      ['b', mssql.NVarChar(50)]
    ])
  },
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
  query: (args, model) => `SELECT '${model.FirstName}' AS FirstName, '${model.LastName}' AS LastName, 'derp' AS hurr`
});

Test.mapQuery({
  static: true,
  oneResult: true,
  name: 'badquery',
  query: (args, model) => `SELECT PJN ON '${model.FirstName}' AS FirstName, '${model.LastName}' AS LastName, 'derp' AS hurr`
});
  
lab.experiment('testing functions', () => {
  
  lab.test('create temp table proc', (done) => {
    Test.getDB((db) => {
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
  
  lab.test('create user defined table proc', (done) => {
    Test.getDB((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      request.query(`IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'customtype') AND type IN (N'P', N'PC')) DROP PROCEDURE customtype`).then(() => {
      }).then(() => {
        return request.query(`IF EXISTS (SELECT * FROM sys.types WHERE is_user_defined=1 AND name='TestUserType') DROP TYPE TestUserType`);
      }).then(() => {
        return request.query(`CREATE TYPE TestUserType AS TABLE (a NVarChar(50), b NVarChar(50))`);
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
    Test.getDB((db) => {
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
  
  lab.test('create multiresults proc', (done) => {
    Test.getDB((db) => {
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

  lab.test('loaded statements', (done) => {
    Promise.all([p1, p2, p3]).then(() => {
      done();
    }).catch(done);
  });

  lab.test('loading stored procs', (done) => {
    Test.getDB((db) => {
      const request = new mssql.Request(db);
      request.query(`
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'testproc') AND type IN (N'P', N'PC'))
DROP PROCEDURE testproc;`).then(() => {
        const request2 = new mssql.Request(db);
        return request2.batch(
`
CREATE PROCEDURE testproc
  @FirstName nvarchar(50),
  @LastName nvarchar(50)
AS
  
  SELECT @LastName AS LastName, @FirstName AS FirstName;
`)  }).then(() => {
        done();
      }).catch(done);
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
    const test = Test.create({
      FirstName: 'Nathanael',
      LastName: 'Fritzer'
    });
    test.testproc().then((results) => {
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
    const test = Test.create({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    });
    test.teststate().then((results) => {
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
    const test = Test.create({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    });
    test.instancequery().then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });
  
  lab.test('multi static statment', (done) => {
    Test.multiselect().then((results) => {
      expect(results[0].FirstName).to.equal('Nathan');
      expect(results[0].LastName).to.equal('Fritz');
      expect(results.length).to.equal(3);
      done();
    }).catch(done);
  });
  
  lab.test('unfound static statment', (done) => {
    Test.getuno({LAST_NAME: 'Derpy'}).then((results) => {
      done('should have errored');
    }).catch((err) => {
      expect(err).to.be.an.instanceof(Seaquell.EmptyResult);
      done();
    });
  });
  
  lab.test('single static statment', (done) => {
    Test.getuno({LAST_NAME: 'Fritz'}).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch(done);
  });

  lab.test('join multi', (done) => {
    const Item = new Seaquell.Model({
      'name_id': {},
      'name': {},
      'weight': {}
    }, {name: 'jm_item', cache: true});
    const Name = new Seaquell.Model({
      'id': {},
      'first': {},
      'last': {},
      'items': { collection: 'jm_item', local: 'id', 'remote': 'name_id' }
    }, {name: 'rm_name', cache: true});
    Name.mapProcedure({
      static: true,
      name: 'multiresult',
      oneResult: false,
      resultModels: ['', 'jm_item']
    });
    Name.multiresult()
    .then((names) => {
      expect(names[0].items[1].name).to.equal('lettuce');
      expect(names[2].items[1].name).to.equal('hair');
      done();
    }).catch(done);
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

  lab.test('queryResults error', (done) => {
    Test._queryResults({}, 'ERROR', [], 0, Promise.resolve, function () {
      done();
    });
  });

  lab.test('no args statement', (done) => {
    const test = Test.create();
    test.noargs()
    .then((model) => {
      expect(model.FirstName).to.equal('Billy');
      expect(model.LastName).to.equal('Bob');
      done();
    });
  });
  
  lab.test('custom type static', (done) => {
    Test.customtype({id: 1, SomeSub: [{a: 'Billy', b: 'Bob',}, {a: 'Ham', b: 'Sammich'}]}).then((model) => {
      expect(model[0].FirstName).to.equal('Billy');
      expect(model[0].LastName).to.equal('Bob');
      expect(model[1].FirstName).to.equal('Ham');
      expect(model[1].LastName).to.equal('Sammich');
      done();
    }).catch(done);
  });
  
  lab.test('custom type method', (done) => {
    const HasSubType = new Seaquell.Model({
      id: {},
      SomeSub: {collection: 'UserType'}
    });
    const UserType = new Seaquell.Model({
      a: {},
      b: {}
    }, {
      name: 'UserType',
      cache: true
    });

    HasSubType.mapProcedure({
      static: false,
      name: 'customtype',
      args: {
        id: mssql.BigInt,
        SomeSub: Seaquell.TVP([
          ['a', mssql.NVarChar(50)],
          ['b', mssql.NVarChar(50)]
        ])
      },
      resultModels: ['Test']
    });
    const model = HasSubType.create({id: 1, SomeSub: [{a: 'Billy', b: 'Bob',}, {a: 'Ham', b: 'Sammich'}]});
    model.customtype().then((rmodel) => {
      expect(rmodel[0].FirstName).to.equal('Billy');
      expect(rmodel[0].LastName).to.equal('Bob');
      expect(rmodel[1].FirstName).to.equal('Ham');
      expect(rmodel[1].LastName).to.equal('Sammich');
      done();
    }).catch(done);
  });

  lab.test('disconnect', (done) => {
    Test.getDB((db) => {
      Test.unprepare('teststate').then(() => {
        db.close();
        done();
      });
    });
  });
});
