var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');

function parsePkgInfo(filename, callback) {
  var parser = function(stdout, done) {
    var info = { 'raw': stdout };
    var lines = stdout.split(/\r?\n/);

    lines.forEach(function(line) {
      var parts = line.split(/=/, 2);

      switch (parts[0]) {
        case 'CATEGORIES':
          info.categories = parts[1].split(/\s+/);
        break;
      }
    });

    done(null, info);
  }

  var pkg_info = spawn('pkg_info', ['-X', filename]);
  var stdout = '';

  pkg_info.stdout.on('data', function (data) {
    stdout += data;
  });

  pkg_info.on('close', function(code, signal) {
    parser(stdout, function(err, info) {
      callback(err, info);
    });
  });
}

function Repository(basedir) {
  var me = this;

  this.basedir = basedir;

  this.packages = {};
  this.categories = {};

  fs.readdir(basedir, function(err, files) {
    files.forEach(function (filename) {
      me.addFile(path.resolve(path.join(basedir, filename)));
    });
  });
}

Repository.prototype.addFile = function(filename, done) {
  var me = this;

  var basename = path.basename(filename);

  if (!done) {
    done = noop;
  }

  parsePkgInfo(filename, function(err, info) {
    me.packages[basename] = info;

    info.categories.forEach(function(category) {
      if (!me.categories[category]) {
        me.categories[category] = {};
      }

      me.categories[category][basename] = info;

      done(err);
    });
  });
}

Repository.prototype.pathForFilename = function(filename) {
  return path.join(this.basedir, filename);
}

Repository.prototype.getCategories = function() {
  return Object.keys(this.categories);
}

Repository.prototype.getPackages = function(category) {
  if (!category || category === 'All') {
    return this.packages;
  } else if (this.categories[category]) {
    return this.categories[category];
  } else {
    return {};
  }
}

Repository.prototype.createPkgSummary = function() {
  var summary = '';

  for (filename in this.packages) {
    summary += this.packages[filename].raw;
    summary += "\n";
  }

  return summary;
};

function noop() {};

module.exports = {
  'parsePkgInfo': parsePkgInfo,
  'Repository': Repository
}
