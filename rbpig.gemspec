Gem::Specification.new do |s|
  s.name = "rbpig"
  s.version = "0.0.1"
  s.date = "2010-12-08"
  
  s.homepage = %q{http://github.com/jae/rbpig}  
  s.summary = "Pig queries execution ruby binding"
  s.description = "Simple lib for executing Pig queries, supports textfile based hive table loader with automatic schema discovery"
    
  s.authors = ["Forward Internet Group"]  
  s.email = "jae.lee@forward.co.uk"
  
  s.rubygems_version = "1.3.5"  
  s.require_paths = ["lib"]
  s.add_dependency('mandy', '>= 0.5.0')
  
  s.files = [
    "lib/rbpig.rb",
    "lib/rbpig/dataset.rb",
    "java/dist/piggybank.jar",
    "java/conf/hive-default.xml",
    "java/conf/hive-site.xml",
    "java/lib/hive/hive-exec-0.5.0+32.jar",
    "java/lib/hive/hive-metastore-0.5.0+32.jar",
    "java/lib/hive/libfb303.jar",
    "java/lib/hive/jdo2-api-2.3-SNAPSHOT.jar",
    "java/lib/hive/datanucleus-core-1.1.2-patched.jar",
    "java/lib/hive/datanucleus-enhancer-1.1.2.jar",
    "java/lib/hive/datanucleus-rdbms-1.1.2.jar",
    "java/lib/pig/jsp-2.1-6.1.14.jar"
  ]
end