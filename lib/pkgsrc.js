var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var sqlite3 = require('sqlite3');

var log = require('bunyan').createLogger({
  name: 'pkgsrc-repository',
  stream: process.stdout,
  level: 'info'
});

var SQL_CACHE_TABLE_CREATE = [
  'CREATE TABLE IF NOT EXISTS packages (',
  'filename TEXT PRIMARY KEY,',
  'filesize INTEGER,',
  'pkgname TEXT,',
  'package_name TEXT,',
  'package_version TEXT,',
  'updated_at INTEGER DEFAULT (strftime(\'%s\',\'now\')),',
  'categories TEXT,',
  'info TEXT);',
  'CREATE INDEX IF NOT EXISTS idx_package_name ON packages (',
    'package_name ASC',
  ');',
  'CREATE INDEX IF NOT EXISTS idx_updated_at ON packages (',
    'updated_at DESC',
  ');'
].join(' ');

function parsePkgInfo(filename, callback) {
  var parser = function(stdout, done) {
    var info = { 'raw': stdout };
    var lines = stdout.split(/\r?\n/);

    lines.forEach(function(line) {
      var parts = line.split(/=/, 2);
      var m;

      switch (parts[0]) {
        case 'PKGNAME':
          info.pkgname = parts[1];

          m = info.pkgname.match(/(.+)-([^-]+)$/);
          if (m) {
            info.package_name = m[1];
            info.package_version = m[2];
          }
        break;
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

function Repository(id, basedir, tmpdir) {
  var me = this;

  this.basedir = basedir;

  this.packages = {};
  this.categories = {};
  this.db = new sqlite3.Database(path.join(tmpdir, id + '_cache.db'));

  this.db.run(SQL_CACHE_TABLE_CREATE, function (err) {
    if (err) {
      log.error({ "error": err }, 'failed creating the cache table');
    }

    fs.readdir(basedir, function(err, files) {
      if (err) {
        log.error({ "error": err }, 'could not read repository directory');
      }

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
    if (err) {
      log.error({ "error": err }, 'error getting fstat');
    }

    me.db.get('SELECT * FROM packages WHERE filename=$filename AND filesize=$filesize LIMIT 1;', {
      $filename: basename,
      $filesize: stats.size
    }, function (err, row) {
      if (err) {
        log.error({ "error": err }, 'error retrieving cache entry from database');
      }

      if (row) {
        var info = {
          "raw": row['info'],
          "pkgname": row['pkgname'],
          "package_name": row['package_name'],
          "package_version": row['package_version'],
          "categories": row['categories'].split(','),
          "basename": basename
        };

        registerPackage(me, info);

        done(err);
      } else {
        log.debug({ "filename": filename, "basename": basename }, 'running pkg_info');

        parsePkgInfo(filename, function(err, info) {
          info["basename"] = basename;

          registerPackage(me, info);

          me.db.run('INSERT OR REPLACE INTO packages (filename, filesize, pkgname, categories, package_name, package_version, info) VALUES ($filename, $filesize, $pkgname, $categories, $package_name, $package_version, $info);', {
            $filename: basename,
            $filesize: stats.size,
            $pkgname: info.pkgname,
            $package_name: info.package_name,
            $package_version: info.package_version,
            $categories: info.categories.join(','),
            $info: info.raw
          }, done);
        });
      }
    });
  });
}

Repository.prototype.removeFile = function(basename, done) {
  var me = this;

  if (!done) {
    done = noop;
  }

  function unregisterPackage(repository, info) {
    if (repository.packages[info.basename]) {
      delete(repository.packages[info.basename]);
    }

    info.categories.forEach(function(category) {
      if (repository.categories[category]) {
        if (repository.categories[category][info.basename]) {
          delete(repository.categories[category][info.basename]);
        }
      }
    });
  }

  me.db.get('SELECT * FROM packages WHERE filename=$filename LIMIT 1;', {
    $filename: basename
  }, function (err, row) {
    if (err) {
      log.error({ "error": err }, 'error retrieving cache entry from database');
    }

    if (row) {
      var info = {
        "raw": row['info'],
        "pkgname": row['pkgname'],
        "package_name": row['package_name'],
        "package_version": row['package_version'],
        "categories": row['categories'].split(','),
        "basename": basename
      };

      me.db.run('DELETE FROM packages WHERE filename=$filename;', {
        $filename: basename
      }, function (err) {
        if (err) {
          log.error({ "error": err }, 'error deleting cache entry from database');
        }

        unregisterPackage(me, info);
      });
    }

    if (fs.existsSync(me.pathForFilename(basename))) {
      fs.unlinkSync(me.pathForFilename(basename));
    }

    done(err);
  });
}

Repository.prototype.prunePackageVersions = function(keep_versions_count) {
  var me = this;

  me.db.all('SELECT package_name FROM packages GROUP BY package_name HAVING COUNT(package_version)>$keep_count;', {
    $keep_count: keep_versions_count
  }, function (err, package_names_row) {
    if (err) {
      log.error({ "error": err }, 'error retrieving prunable package names from database');
    } else {
      package_names_row.forEach(function (row) {
        me.db.all('SELECT filename FROM packages WHERE package_name=$package_name ORDER BY updated_at DESC LIMIT $keep_count,100;', {
          $keep_count: keep_versions_count,
          $package_name: row.package_name
        }, function (err, filenames_row) {
          if (err) {
            log.error({ "error": err }, 'error retrieving prunable filenames from database');
          } else {
            filenames_row.forEach(function(row) {
              me.removeFile(row.filename);
            });
          }
        });
      });
    }
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
