# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "nysol-meachi"
  spec.version       = "3.0.0"
  spec.authors       = ["nysol"]
  spec.email         = ["info@nysol.jp"]
  spec.platform      = Gem::Platform.local
  spec.summary       = %q{nysol Parallel processing tools}
  spec.description   = %q{refer : http://www.nysol.jp/}
  spec.homepage      = "http://www.nysol.jp/"
	spec.extensions = ['ext/meachi/extconf.rb']
  spec.files         = Dir.glob(["lib/nysol/meachi.*"])
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
	spec.add_development_dependency 'rake-compiler'

end
