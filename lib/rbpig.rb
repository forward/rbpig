require 'fileutils'
require File.join(File.dirname(__FILE__), "rbpig", "dataset")

module RBPig
  class << self
    CLASSPATH = [
      "#{File.join(File.dirname(__FILE__), %w[..  java dist piggybank.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive hive-exec-0.5.0+32.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive hive-metastore-0.5.0+32.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive libfb303.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive jdo2-api-2.3-SNAPSHOT.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive datanucleus-core-1.1.2-patched.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive datanucleus-enhancer-1.1.2.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib hive datanucleus-rdbms-1.1.2.jar])}",
      "#{File.join(File.dirname(__FILE__), %w[..  java lib pig jsp-2.1-6.1.14.jar])}",        
      "#{File.join(File.dirname(__FILE__), %w[..  java conf])}"
    ].join(":").freeze
    
    def classpath
      CLASSPATH
    end
    
    def datasets(*datasets, &blk)
      yield Pig.new(datasets) unless blk.nil?
    end
  end
  
  class Pig
    def initialize(datasets)
      @oink_oink = [*datasets.map{|e| e.to_s}]
    end
    
    def grunt(oink)
      @oink_oink << oink
    end
    
    def fetch(alias_to_fetch)
      alias_dump = "/tmp/pigdump/#{Process.pid}_#{Time.now.to_i}"
      
      FileUtils.mkdir_p("/tmp/pigscript")
      script_file = "/tmp/pigscript/#{Process.pid}_#{Time.now.to_i}"
      File.open(script_file, "w") do |file|
        @oink_oink << "STORE #{alias_to_fetch} INTO '#{alias_dump}';"
        file << @oink_oink.join("\n")
      end
      
      pig_execution = "PIG_CLASSPATH='#{RBPig.classpath}' pig -f #{script_file}"
      if system(pig_execution)
        local_alias_dump = alias_dump
        File.delete(local_alias_dump) if File.exists?(local_alias_dump)
        `mandy-get #{alias_dump} #{local_alias_dump}`
        `mandy-rm #{alias_dump}`
        File.open(local_alias_dump).readlines
      else
        raise "Failed executing #{pig_execution}"
      end
    end
  end
end