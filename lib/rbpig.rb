require 'fileutils'
require 'rexml/document'
require File.join(File.dirname(__FILE__), "rbpig", "dataset")

module RBPig
  class << self
    def executable(hadoop_config_file=nil)
      pig_options = ["-Dudf.import.list=forward.pig.storage"]
      unless hadoop_config_file.nil?
        hadoop_config = {}
        REXML::Document.new(File.new(hadoop_config_file)).elements.each('configuration/property') do |property|
          hadoop_config[property.elements[1].text] = property.elements[2].text
        end
        pig_options << "-Dfs.default.name=#{hadoop_config["fs.default.name"]}" if hadoop_config.has_key?("fs.default.name")
        pig_options << "-Dmapred.job.tracker=#{hadoop_config["mapred.job.tracker"]}" if hadoop_config.has_key?("mapred.job.tracker")
      end
      ["PIG_CLASSPATH='#{classpath}'", "PIG_OPTS='#{pig_options.join(" ")}'", "pig"].join(" ")
    end
    
    def connect(hadoop_config_file=nil)
      yield Pig.new(hadoop_config_file)
    end
    
    private
    def classpath
      @classpath ||= [
        "#{File.join(File.dirname(__FILE__), %w[.. java dist porkchop.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive hive-exec-0.5.0+32.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive hive-metastore-0.5.0+32.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive libfb303.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive jdo2-api-2.3-SNAPSHOT.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-core-1.1.2-patched.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-enhancer-1.1.2.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-rdbms-1.1.2.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib pig jsp-2.1-6.1.14.jar])}",        
        "#{File.join(File.dirname(__FILE__), %w[.. bin])}"
      ].join(":").freeze
    end
  end
  
  class Pig
    def initialize(hadoop_config_file)
      @hadoop_config_file = hadoop_config_file
      @oink_oink = []
    end
    
    def datasets(*datasets)
       datasets.each {|e| @oink_oink << e.to_s}
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
      pig_execution = "#{RBPig.executable(@hadoop_config_file)} -f #{script_file}"
      if system(pig_execution)
        local_alias_dump_dir = alias_dump_dir
        FileUtils.rm_rf(local_alias_dump_dir) if File.exists?(local_alias_dump_dir)
        
        mandy_config = @hadoop_config_file.nil? && "" || "-c #{@hadoop_config_file}"
        aliases.each do |alias_to_fetch|
          `mandy-get #{mandy_config} #{alias_dump_dir}/#{alias_to_fetch} #{local_alias_dump_dir}/#{alias_to_fetch}`
          alias_dumps << File.open("#{local_alias_dump_dir}/#{alias_to_fetch}").readlines.map{|e| e.chomp("\n").split("\t", -1)}
        end
        `mandy-rm #{mandy_config} #{alias_dump_dir}`
        return *alias_dumps
      else
        raise "Failed executing #{pig_execution}"
      end
    end
  end
end