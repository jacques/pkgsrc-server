var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var mkdirp = require('mkdirp');
var sqlite3 = require('sqlite3');

var log = require('bunyan').createLogger({
  name: 'pkgsrc-repository',
  stream: process.stdout,
  level: 'info'
});

var SQL_DB_INIT = [
  'PRAGMA foreign_keys=ON;',
  'CREATE TABLE IF NOT EXISTS files (',
    'filename TEXT PRIMARY KEY,',
    'filesize INTEGER',
  ');',
  'CREATE TABLE IF NOT EXISTS categories (',
    'filename TEXT REFERENCES files(filename) ON UPDATE CASCADE ON DELETE CASCADE,',
    'category TEXT',
  ');',
  'CREATE TABLE IF NOT EXISTS info (',
    'filename TEXT REFERENCES files(filename) ON UPDATE CASCADE ON DELETE CASCADE,',
    'pkgname TEXT,',
    'package_name TEXT,',
    'package_version TEXT,',
    'updated_at INTEGER DEFAULT (strftime(\'%s\',\'now\')),',
    'info TEXT DEFAULT \'\'',
  ');',
  'CREATE INDEX IF NOT EXISTS idx_category ON categories (',
    'category ASC',
  ');',
  'CREATE INDEX IF NOT EXISTS idx_package_name ON info (',
    'package_name ASC',
  ');',
  'CREATE INDEX IF NOT EXISTS idx_updated_at ON info (',
    'updated_at DESC',
  ');',
  'CREATE TRIGGER IF NOT EXISTS insert_new_files AFTER INSERT ON files',
  'BEGIN',
    'INSERT INTO info (filename) VALUES (new.filename);',
  'END;',
''].join("\n");

function getPkgInfo(filenames, callback, done) {
  var parser = function(lines, filename, done) {
    var info = {
      'filename': path.basename(filename),
      'raw': lines.join("\n")
    };

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

  log.info('running pkg_info for %d files', filenames.length);

  var pkg_info = spawn('pkg_info', ['-X'].concat(filenames));
  var stdout = '';

  pkg_info.stdout.on('data', function (data) {
    stdout += data;
  });

  pkg_info.on('close', function(code, signal) {
    var buffer = [];
    var filenames_idx = 0;

    stdout.split(/\r?\n/).forEach(function(line) {
      if (line === '' && buffer.length > 0) {
        parser(buffer, filenames[filenames_idx++], function(err, info) {
          callback(err, info);
        });

        buffer = [];
      } else {
        buffer.push(line);
      }
    });

    pkg_info.kill();

    done();
  });

}

function Repository(id, basedir, tmpdir) {
  var me = this;

  this.basedir = basedir;

  this.categories = {};
  this.db = new sqlite3.Database(path.join(tmpdir, id + '_cache.db'));

  this.db.exec(SQL_DB_INIT, function (err) {
    if (err) {
      log.error({ "error": err }, 'failed executing the database init sql');
    }

    mkdirp(me.basedir, function (err) {
      if (err) {
        log.error({ "error": err }, 'could not create repository directory');
      }
    });
  });
}

Repository.prototype.reload = function(done) {
  var me = this;

  if (!done) {
    done = noop;
  }

  fs.readdir(me.basedir, function(err, files) {
    if (err) {
      log.error({ "error": err }, 'could not read repository directory');
    }

    log.info('listing packages in repository');
    async.eachLimit(files, 16, function (filename, file_done) {
      if (filename.match(/\.tgz$/i) && !filename.match(/^pkg_summary\./i)) {
        me.addFile(path.resolve(path.join(me.basedir, filename)), file_done);
      } else {
        file_done(null);
      }
    }, function (err) {
      if (err) {
        log.error({ "error": err }, 'there was a problem adding files to the repository');
      }

      log.info('resync for pkg_info is executed in the background');

      // determine which files need their pkg_info and categories recomputed
      var queue = [];
      me.db.each('SELECT filename FROM info WHERE info=\'\';', function (err, row) {
        if (err) {
          log.error({ "error": err }, 'there was a problem getting all filenames to process');
        }

        queue.push(me.pathForFilename(row.filename));
      }, function (err, queue_size) {
        log.info('got %d items in queue to get pkg_info data for', queue_size);

        var i;
        var block_size = 100;
        var work_blocks = [];

        for (i = 0; i < queue_size; i += block_size) {
          work_blocks.push(queue.slice(i, i + block_size));
        }

        async.eachSeries(work_blocks, function (filenames, block_done) {
          getPkgInfo(filenames, function (err, info) {
            me.db.run('UPDATE info SET pkgname=$pkgname, package_name=$package_name, package_version=$package_version, info=$info WHERE filename=$filename;', {
              $pkgname: info.pkgname,
              $package_name: info.package_name,
              $package_version: info.package_version,
              $info: info.raw,
              $filename: info.filename,
            }, function (err) {
              if (err) {
                log.error({ "error": err }, 'there was a problem updating the info table');
              }
            });

            info.categories.forEach(function(category) {
              me.db.run('INSERT OR REPLACE INTO categories (filename,category)VALUES($filename,$category);', {
                $filename: info.filename,
                $category: category
              }, function (err) {
                if (err) {
                  log.error({ "error": err }, 'there was a problem updating the categories table');
                }
              });
            });
          }, function (err) {
            block_done(err);
          });
        }, function (err) {
          log.info('pkg_info data has been updated');
        });
      });

      done(err);
    });
  });
}

Repository.prototype.addFile = function(filename, done) {
  var me = this;

  var basename = path.basename(filename);

  if (!done) {
    done = noop;
  }

  fs.stat(filename, function (err, stats) {
    if (err) {
      log.error({ "error": err }, 'error getting fstat');
    }

    me.db.get('SELECT * FROM files WHERE filename=$filename AND filesize=$filesize LIMIT 1;', {
      $filename: basename,
      $filesize: stats.size
    }, function (err, row) {
      if (err) {
        log.error({ "error": err }, 'error retrieving cache entry from database');
      }

      if (!row) {
        me.db.run('INSERT OR REPLACE INTO files (filename,filesize) VALUES ($filename,$filesize);', {
          $filename: basename,
          $filesize: stats.size
        }, done);
      } else {
        done(err);
      }
    });
  });
}

Repository.prototype.removeFile = function(basename, done) {
  var me = this;

  if (!done) {
    done = noop;
  }

  me.db.get('SELECT * FROM files WHERE filename=$filename LIMIT 1;', {
    $filename: basename
  }, function (err, row) {
    if (err) {
      log.error({ "error": err }, 'error retrieving cache entry from database');
    }

    if (row) {
      me.db.run('DELETE FROM files WHERE filename=$filename;', {
        $filename: basename
      }, function (err) {
        if (err) {
          log.error({ "error": err }, 'error deleting cache entry from database');
        }
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

  me.db.all('SELECT package_name FROM info GROUP BY package_name HAVING COUNT(package_version)>$keep_count;', {
    $keep_count: keep_versions_count
  }, function (err, package_names_row) {
    if (err) {
      log.error({ "error": err }, 'error retrieving prunable package names from database');
    } else {
      package_names_row.forEach(function (row) {
        me.db.all('SELECT filename FROM info WHERE package_name=$package_name ORDER BY updated_at DESC LIMIT $keep_count,100;', {
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

Repository.prototype.getCategories = function(callback) {
  var me = this;

  var categories = [];

  me.db.each('SELECT DISTINCT category FROM categories ORDER BY category;', function(err, row) {
    categories.push(row.category);
  }, function (err, categories_count) {
    callback(err, categories);
  });
}

Repository.prototype.getPackages = function(category, callback) {
  var me = this;

  if (typeof category === 'function') {
    callback = category;
    category = null;
  }

  var packages = [];

  function _each_package(err, row) {
    packages.push(row.filename);
  }

  function _done_packages(err, packages_count) {
    callback(err, packages);
  }

  if (!category || category === 'All') {
    me.db.each('SELECT DISTINCT filename FROM categories ORDER BY filename;',
      _each_package, _done_packages);
  } else  {
    me.db.each('SELECT DISTINCT filename FROM categories WHERE category=$category ORDER BY filename;', {
      $category: category
    }, _each_package, _done_packages);
  }
}

Repository.prototype.createPkgSummary = function(callback) {
  var me = this;

  var summary = '';

  me.db.each('SELECT info FROM info;', function(err, row) {
    summary += row.info;
    summary += "\n\n";
  }, function (err, packages_count) {
    callback(err, summary);
  });
};

function noop() {};

module.exports = {
  'getPkgInfo': getPkgInfo,
  'Repository': Repository
}
