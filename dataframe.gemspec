# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dataframe/version'

Gem::Specification.new do |spec|
  spec.name          = "dataframe"
  spec.version       = Dataframe::VERSION
  spec.authors       = ["Claus Dahl"]
  spec.email         = ["dee@classy.dk"]
  spec.summary       = %q{Lazy chainable data processing for table oriented data}
  spec.description   = %q{Provides useful chainable data munging for data mining use. Compute columns, reshape table data etc.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "pry"
end
