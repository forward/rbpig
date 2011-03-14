Gem::Specification.new do |s|
  s.name = "rbpig"
  s.version = "0.1.2"
  s.date = "2011-03-10"
  
  s.homepage = %q{https://github.com/forward/rbpig}
  s.summary = "Pig queries execution ruby binding"
  s.description = "Simple lib for executing Pig queries, supports textfile based hive table loader with automatic schema discovery"
    
  s.authors = ["Jae Lee"]  
  s.email = "jlee@yetitrails.com"
  
  s.rubygems_version = "1.3.5"  
  s.require_paths = ["lib"]
  s.add_dependency('mandy', '>= 0.5.0')
  
  s.executables = ["rbpig"]
  s.files = [
    "bin/rbpig",
    "lib/rbpig.rb",
    "lib/rbpig/dataset.rb",
    "java/dist/porkchop.jar",
    "java/lib/hive/hive-exec-0.7.0-CDH3B4.jar",
    "java/lib/hive/hive-metastore-0.7.0-CDH3B4.jar",
    "java/lib/hive/libfb303.jar",
    "java/lib/hive/jdo2-api-2.3-ec.jar",
    "java/lib/hive/datanucleus-core-2.0.3.jar",
    "java/lib/hive/datanucleus-enhancer-2.0.3.jar",
    "java/lib/hive/datanucleus-rdbms-2.0.3.jar",
    "java/lib/hive/mysql-connector-java-5.1.15-bin.jar",
    "java/lib/pig/jsp-2.1.jar",
    "java/lib/pig/antlr-3.3-complete.jar"
  ]
end
