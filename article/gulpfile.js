var elixir = require('laravel-elixir');

elixir.config.publicPath = 'public';
elixir.config.assetsPath = '';
elixir.config.css.less.pluginOptions = {paths:['node_modules']};

elixir(function(mix) {
    mix.less('main.less');
    mix.browserify('main.js');
});