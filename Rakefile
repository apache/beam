require 'html-proofer'

task :test do
  sh "bundle exec jekyll build"
  HTMLProofer.check_directory("./content", {
    :allow_hash_href => true,
    :check_html => true,
    :file_ignore => [/javadoc/]
    }).run
end
