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

const p3 = Test.mapProcedure({
  static: true,
  name: 'multiselect',
  oneResult: false,
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
  
lab.experiment('testing functions', () => {
  
  lab.test('create temp table', (done) => {
    Test.getDB((db) => {
      const request = new mssql.Request(db);
      request.multiple = true;
      const q1 =request.query(`
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'multiselect') AND type IN (N'P', N'PC'))
DROP PROCEDURE multiselect`);
      const r2 = new mssql.Request(db);
      const q2 = r2.query(`
CREATE PROCEDURE multiselect
AS
  CREATE TABLE #TempTest (FIRST_NAME VARCHAR(50), LAST_NAME VARCHAR(50));
  INSERT INTO #TempTest (FIRST_NAME, LAST_NAME) VALUES ('Nathan', 'Fritz'), ('Robert', 'Robles'), ('Cow', 'Town');
  SELECT * FROM #TempTest;
`);
      Promise.all([q1, q2]).then(() => {
        done();
      }).catch((e) => {
        console.log(e);
        console.log(e.stack);
        done();
      });
    });
  });
  
  lab.test('loaded statements', (done) => {
    Promise.all([p1, p2, p3]).then(() => {
      done();
    }).catch((err) => {
      console.log(err.stack);
    });
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
      }).catch((err) => {
        console.log(err.stack);
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
    const test = Test.create({
      FirstName: 'Nathanael',
      LastName: 'Fritzer'
    });
    test.testproc().then((results) => {
      expect(results.FirstName).to.equal('Nathanael');
      expect(results.LastName).to.equal('Fritzer');
      done();
    }).catch((e) => {
      console.log(e.stack);
    });
  });

  lab.test('static statement', (done) => {
    Test.teststate({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch((err) => {
      console.log(err.stack);
      done();
    });
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
    }).catch((err) => {
      console.log(err.stack);
      done();
    });
  });
  
  lab.test('static query', (done) => {
    Test.staticquery({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    }).catch((err) => {
      console.log(err.stack);
      done();
    });
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
    }).catch((err) => {
      console.log(err.stack);
    });
  });
  
  lab.test('multi static statment', (done) => {
    Test.multiselect().then((results) => {
      expect(results[0].FirstName).to.equal('Nathan');
      expect(results[0].LastName).to.equal('Fritz');
      expect(results.length).to.equal(3);
      done();
    }).catch((err) => {
      console.log(err.stack);
    });
  });

  lab.test('disconnect', (done) => {
    Test.getDB((db) => {
      db.close();
      done();
    });
  });
});
