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
  FirstName: {},
  LastName: {},
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
  
  lab.test('loaded statements', (done) => {
    Promise.all([p1, p2]).then(() => {
      done();
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

  lab.test('static proceedure', (done) => {
    Test.testproc({
      FirstName: 'Nathan',
      LastName: 'Fritz',
    }).then((results) => {
      expect(results.FirstName).to.equal('Nathan');
      expect(results.LastName).to.equal('Fritz');
      done();
    });
  });

  lab.test('instance proceedure', (done) => {
    const test = Test.create({
      FirstName: 'Nathanael',
      LastName: 'Fritzer'
    });
    test.testproc().then((results) => {
      expect(results.FirstName).to.equal('Nathanael');
      expect(results.LastName).to.equal('Fritzer');
      done();
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

  lab.test('disconnect', (done) => {
    Test.getDB((db) => {
      db.close();
      done();
    });
  });
});
