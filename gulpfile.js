var elixir = require('laravel-elixir');

/*
 |--------------------------------------------------------------------------
 | Elixir Asset Management
 |--------------------------------------------------------------------------
 |
 | Elixir provides a clean, fluent API for defining some basic Gulp tasks
 | for your Laravel application. By default, we are compiling the Less
 | file for our application, as well as publishing vendor resources.
 |
 */

elixir.config.publicPath = '';
elixir.config.assetsPath = '';
elixir.config.css.less.pluginOptions = {paths:['vendor', 'node_modules']};

elixir(function(mix) {
    mix.less('main.less');

    mix.scripts(['highlight.js/highlight.pack.js'], 'js/vendors.js', 'vendor');
});