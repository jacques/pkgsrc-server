'use strict';

var express = require('express');

var log = require('bunyan').createLogger({
  name: 'pkgsrc-server',
  stream: process.stdout,
  level: 'info'
});

var argv = require('minimist')(process.argv.slice(2));
var config = require(argv['config'] || './config.json');
var app = express();


app.set('x-powered-by', false);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');

app.use(require('express-bunyan-logger')({
  name: 'logger', 
  streams: [{
    level: 'info',
    stream: process.stdout
  }]
}));

// mount repositories
for (var repo_id in config['repos']) {
  var repo_config = config['repos'][repo_id];
  log.info({ 'config': repo_config }, 'creating sub app for repo ' + repo_id);

  app.use(repo_config['mount'],
          require('./lib/repository')(repo_id, config, repo_config));
}

// catchall for redirecting into the 'good-paths'
app.use(function(req, res){
  res.render('repository_list', {
    'repos': config['repos']
  });
});

app.listen(config['port'] || 8197, config['address'] || '0.0.0.0', function() {
  log.info('server started');
});
