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
    
    def fetch(*aliases)
      alias_dump_dir = "/tmp/pigdump/#{Process.pid}_#{Time.now.to_i}"
      FileUtils.mkdir_p("/tmp/pigscript")
      script_file = "/tmp/pigscript/#{Process.pid}_#{Time.now.to_i}"
      
      File.open(script_file, "w") do |file|
        aliases.each do |alias_to_fetch|
          @oink_oink << "STORE #{alias_to_fetch} INTO '#{alias_dump_dir}/#{alias_to_fetch}' USING PigStorage ('\\t');"
        end
        file << @oink_oink.join("\n")
      end
      
      alias_dumps = []
      pig_execution = "PIG_CLASSPATH='#{RBPig.classpath}' pig -f #{script_file}"
      if system(pig_execution)
        local_alias_dump_dir = alias_dump_dir
        FileUtils.rm_rf(local_alias_dump_dir) if File.exists?(local_alias_dump_dir)
        
        aliases.each do |alias_to_fetch|
          `mandy-get #{alias_dump_dir}/#{alias_to_fetch} #{local_alias_dump_dir}/#{alias_to_fetch}`
          alias_dumps << File.open("#{local_alias_dump_dir}/#{alias_to_fetch}").readlines.map{|e| e.chomp("\n").split("\t")}
        end
        `mandy-rm #{alias_dump_dir}`
        return *alias_dumps
      else
        raise "Failed executing #{pig_execution}"
      end
    end
  end
end