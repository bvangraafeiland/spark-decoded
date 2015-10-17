var elixir = require('laravel-elixir');

elixir.config.publicPath = '';
elixir.config.assetsPath = '';
elixir.config.css.less.pluginOptions = {paths:['node_modules']};

elixir(function(mix) {
    mix.less('main.less');
    mix.browserify('main.js', 'js/bundle.js');
});