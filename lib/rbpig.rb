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
      ["PIG_CLASSPATH='#{classpath}'", "PIG_OPTS='#{pig_options.join(" ")}'", "pig", "-l /tmp"].join(" ")
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
      aliases = aliases.map {|alias_to_fetch| "#{alias_dump_dir}/#{alias_to_fetch}"}
      
      pig_script_path = "/tmp/pigscript/#{Process.pid}_#{Time.now.to_i}"
      FileUtils.mkdir_p(File.dirname(pig_script_path))
      File.open(pig_script_path, "w") do |file|
        @oink_oink.each {|oink| file << "#{oink}\n"}
        aliases.each do |dump_file_path|
          file << "STORE #{File.basename(dump_file_path)} INTO '#{dump_file_path}' USING PigStorage ('\\t');\n"
        end
      end
      
      pig_execution = "#{RBPig.executable(@hadoop_config_file)} -f #{pig_script_path} 2>&1"
      exec(pig_execution)
      if $?.success?
        return *fetch_files_in_hdfs(aliases).map {|lines| lines.map{|e| e.chomp("\n").split("\t", -1)}}
      else
        raise "Failed executing #{pig_execution}"
      end
    end
    
    private
    def fetch_files_in_hdfs(file_paths)
      mandy_config = @hadoop_config_file.nil? && "" || "-c #{@hadoop_config_file}"
      return file_paths.map do |file_path|
        FileUtils.remove_file(file_path, true) if File.exists?(file_path)
        `mandy-get #{mandy_config} #{file_path} #{file_path}`
        `mandy-rm #{mandy_config} #{file_path}`
        File.open(file_path) {|file| file.readlines}
      end
    end
  end
end