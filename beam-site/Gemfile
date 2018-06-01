# Updates to this file should include a corresponding change to Gemfile.lock.
# See README.md for more info.

source 'https://rubygems.org'

gem 'jekyll', '3.2'

# Jekyll plugins
group :jekyll_plugins do
	gem 'jekyll-redirect-from'
	gem 'jekyll-sass-converter'
	gem 'html-proofer'
	gem 'jekyll_github_sample'
end

# Used by Travis tests.
gem 'rake'

# Force a version lower than 5.0.0.0, which requires a newer ruby than Travis
# supports.
gem 'activesupport', '<5.0.0.0'
