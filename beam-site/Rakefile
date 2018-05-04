require 'fileutils'
require 'html-proofer'
require 'etc'

task :test do
  FileUtils.rm_rf('./.testcontent')
  sh "bundle exec jekyll build --config _config.yml,_config_test.yml"
  HTMLProofer.check_directory("./.testcontent", {
    :typhoeus => {
      :timeout => 60,
      :connecttimeout => 40 },
    :allow_hash_href => true,
    :check_html => true,
    :file_ignore => [/javadoc/, /v2/, /pydoc/],
    :url_ignore => [/jstorm.io/, /datatorrent.com/],
    :parallel => { :in_processes => Etc.nprocessors },
    }).run
end
