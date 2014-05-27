var spawn = require('child_process').spawn;
var fs = require('fs');
var express = require('express');
var multiparty = require('multiparty');
var pkgsrc = require('./pkgsrc');

var log = require('bunyan').createLogger({
  name: 'pkgsrc-server',
  stream: process.stdout,
  level: 'info'
});

var argv = require('minimist')(process.argv.slice(2));
var app = express();

var repository = new pkgsrc.Repository(argv['repository']);

app.set('x-powered-by', false);
app.set('views', __dirname + '/../views');
app.set('view engine', 'jade');

app.use(require('express-bunyan-logger')({
  name: 'logger', 
  streams: [{
    level: 'info',
    stream: process.stdout
  }]
}));

app.get('/', function(req, res) {
  var categories = repository.getCategories();

  res.render('category_list', {
    'items': categories.sort()
  });
});

app.get('/:category', function(req, res) {
  var category = req.params['category'];
  var packages = repository.getPackages(category);

  res.render('package_list', {
    'category': category,
    'items': packages
  });
});

app.get('/All/pkg_summary.gz', function(req, res) {
  res.writeHead(200, {'Content-Type': 'application/octet-stream'});

  var gzip = spawn('gzip', ['-c']);

  gzip.on('exit', function() {
    res.end();
  });

  gzip.stdout.on('data', function (chunk) {
    res.write(chunk);
  });

  gzip.stdin.write(repository.createPkgSummary());
  gzip.stdin.end();
});

app.get('/All/pkg_summary.bz2', function(req, res) {
  res.writeHead(200, {'Content-Type': 'application/octet-stream'});

  var bzip2 = spawn('bzip2', ['-zc']);

  bzip2.on('exit', function() {
    res.end();
  });

  bzip2.stdout.on('data', function (chunk) {
    res.write(chunk);
  });

  bzip2.stdin.write(repository.createPkgSummary());
  bzip2.stdin.end();
});

app.get('/:category/:filename', function(req, res) {
  var category = req.params['category'];
  var filepath = repository.pathForFilename(req.params['filename']);

  res.sendfile(filepath);
});

app.post('/upload', function(req, res) {
  if (req.query['token'] != argv['auth_token']) {
    res.send(403, { 'Error': 'Not authorized.' });
    return
  }

  var form = new multiparty.Form();

  form.parse(req, function(err, fields, files) {
    for (fieldname in files) {
      var field = files[fieldname];

      field.forEach(function (item) {
        fs.rename(item.path, repository.pathForFilename(item.originalFilename));

        repository.addFile(repository.pathForFilename(item.originalFilename), function (err) {
          res.send(200, { 'Status': 'OK' });
        });
      });
    }
  });
});

app.listen(argv['port'] || 3000);
