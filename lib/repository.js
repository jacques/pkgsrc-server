'use strict';
var spawn = require('child_process').spawn;
var fs = require('fs');
var os = require('os');
var express = require('express');
var multiparty = require('multiparty');
var pkgsrc = require('./pkgsrc');
var rsync = require('./rsync');

module.exports = function(id, config, repository_config) {
  var router = express.Router();
  var repository = new pkgsrc.Repository(id,
                                         repository_config['path'],
                                         config.cachedir || os.tmpDir());
  var syncer = new rsync.Rsync(repository_config['rsync_from'], repository_config['path']);

  syncer.run(function (err) {
    repository.reload();
  });

  router.get('/', function(req, res) {
    repository.getCategories(function (err, categories) {
      res.render('category_list', {
        'items': categories
      });
    });
  });

  router.get('/:category', function(req, res) {
    var category = req.params['category'];

    repository.getPackages(category, function(err, packages) {
      res.render('package_list', {
        'category': category,
        'items': packages
      });
    });
  });

  router.get('/All/pkg_summary.gz', function(req, res) {
    var buf = new Buffer('');
    var gzip = spawn('gzip', ['-c']);

    gzip.on('exit', function() {
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Content-Length': buf.length,
        'Last-Modified': (new Date()).toUTCString()
      });

      res.write(buf);
      res.end();
    });

    gzip.stdout.on('data', function (chunk) {
      buf = Buffer.concat([buf, chunk]);
    });

    repository.createPkgSummary(function(err, summary) {
      gzip.stdin.write(summary);
      gzip.stdin.end();
    });
  });

  router.get('/All/pkg_summary.bz2', function(req, res) {
    var buf = new Buffer('');
    var bzip2 = spawn('bzip2', ['-zc']);

    bzip2.on('exit', function() {
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Content-Length': buf.length,
        'Last-Modified': (new Date()).toUTCString()
      });

      res.write(buf);
      res.end();
    });

    bzip2.stdout.on('data', function (chunk) {
      buf = Buffer.concat([buf, chunk]);
    });

    repository.createPkgSummary(function(err, summary) {
      bzip2.stdin.write(summary);
      bzip2.stdin.end();
    });
  });

  router.get('/:category/:filename', function(req, res) {
    var category = req.params['category'];
    var filepath = repository.pathForFilename(req.params['filename']);

    res.sendfile(filepath);
  });

  router.post('/upload', function(req, res) {
    if (req.query['token'] != config['auth_token']) {
      res.send(403, { 'Error': 'Not authorized.' });
      return
    }

    var form = new multiparty.Form({ uploadDir: config['tmpdir'] || os.tmpDir() });

    form.parse(req, function(err, fields, files) {
      for (var fieldname in files) {
        var field = files[fieldname];

        field.forEach(function (item) {
          fs.renameSync(item.path, repository.pathForFilename(item.originalFilename));

          repository.addFile(repository.pathForFilename(item.originalFilename), function (err) {
            if (repository_config['keep_package_versions'] && repository_config['keep_package_versions'] > 0) {
              repository.prunePackageVersions(repository_config['keep_package_versions']);
            }

            res.send(200, { 'Status': 'OK' });
          });
        });
      }
    });
  });

  return router;
}
