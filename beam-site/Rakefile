require 'fileutils'
require 'html-proofer'

task :test do
  FileUtils.rm_rf('./content')
  sh "bundle exec jekyll build --config _config.yml,_config_test.yml"
  HTMLProofer.check_directory("./content", {
    :allow_hash_href => true,
    :check_html => true,
    :file_ignore => [/javadoc/, /v2/]
    }).run
end
