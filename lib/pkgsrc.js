var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var sqlite3 = require('sqlite3');

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

    pkg_info.kill();
  });
}

function Repository(basedir) {
  var me = this;

  this.basedir = basedir;

  this.packages = {};
  this.categories = {};
  this.db = new sqlite3.Database('cache.db');

  this.db.run('CREATE TABLE IF NOT EXISTS packages (filename PRIMARY KEY, filesize INTEGER, categories TEXT, info TEXT);', function (err) {
    fs.readdir(basedir, function(err, files) {
      async.eachLimit(files, 16, function (filename, done) {
        if (filename.match(/\.tgz$/i) && !filename.match(/^pkg_summary\./i)) {
          me.addFile(path.resolve(path.join(basedir, filename)), done);
        }
      });
    });
  });
}

Repository.prototype.addFile = function(filename, done) {
  var me = this;

  var basename = path.basename(filename);

  if (!done) {
    done = noop;
  }

  function registerPackage(repository, info) {
    repository.packages[info.basename] = info;

    info.categories.forEach(function(category) {
      if (!repository.categories[category]) {
        repository.categories[category] = {};
      }

      repository.categories[category][info.basename] = info;
    });
  }

  fs.stat(filename, function (err, stats) {
    me.db.get('SELECT * FROM packages WHERE filename=$filename AND filesize=$filesize LIMIT 1;', {
      $filename: basename,
      $filesize: stats.size
    }, function (err, row) {
      if (row) {
        var info = {
          "raw": row['info'],
          "categories": row['categories'].split(','),
          "basename": basename
        };

        registerPackage(me, info);

        done(err);
      } else {
        console.log('running pkg_info for: ' + basename);

        parsePkgInfo(filename, function(err, info) {
          info["basename"] = basename;

          registerPackage(me, info);

          me.db.run('INSERT OR REPLACE INTO packages (filename, filesize, categories, info) VALUES ($filename, $filesize, $categories, $info);', {
            $filename: basename,
            $filesize: stats.size,
            $categories: info.categories.join(','),
            $info: info.raw
          }, done);
        });
      }
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
