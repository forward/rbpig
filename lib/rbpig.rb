require 'fileutils'
require 'rexml/document'
require File.join(File.dirname(__FILE__), "rbpig", "dataset")

module RBPig
  class << self
    def executable(configs)
      configs = pig_configs(configs)
      
      pig_options = ["-Dudf.import.list=forward.pig.storage"]
      unless configs[:hadoop_config].nil?
        hadoop_config = {}
        REXML::Document.new(File.new(configs[:hadoop_config])).elements.each('configuration/property') do |property|
          hadoop_config[property.elements[1].text] = property.elements[2].text
        end
        pig_options << "-Dfs.default.name=#{hadoop_config["fs.default.name"]}" if hadoop_config.has_key?("fs.default.name")
        pig_options << "-Dmapred.job.tracker=#{hadoop_config["mapred.job.tracker"]}" if hadoop_config.has_key?("mapred.job.tracker")
      end
      ["PIG_CLASSPATH='#{pig_classpath(configs)}'", "PIG_OPTS='#{pig_options.join(" ")}'", "pig", "-l /tmp"].join(" ")
    end
    
    def connect(configs)
      yield Pig.new(pig_configs(configs))
    end
    
    private
    def pig_configs(configs)
      {:hadoop_config => nil, :hive_config => nil}.merge(configs || {})
    end
    
    def pig_classpath(configs)
      classpath = [
        "#{File.join(File.dirname(__FILE__), %w[.. java dist porkchop.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive hive-exec-0.7.0-CDH3B4.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive hive-metastore-0.7.0-CDH3B4.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive libfb303.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive jdo2-api-2.3-ec.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-core-2.0.3.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-enhancer-2.0.3.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive datanucleus-rdbms-2.0.3.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive mysql-connector-java-5.1.15-bin.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib pig jsp-2.1.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib pig antlr-3.3-complete.jar])}"
      ]
      unless configs[:hive_config].nil?
        raise "Rename '#{configs[:hive_config]}' to hive-site.xml for hive metastore configuration." unless File.basename(configs[:hive_config]) == "hive-site.xml"
        classpath << File.dirname(configs[:hive_config])
      end
      classpath.join(":").freeze
    end
  end
  
  class Pig
    def initialize(configs)
      @configs = configs
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
      
      pig_execution = "#{RBPig.executable(@configs)} -f #{pig_script_path} 2>&1"
      pig_out = []
      IO.popen(pig_execution) do |stdout|
        puts pig_execution
        until stdout.eof? do
          pig_out << stdout.gets
          puts pig_out.last
        end
      end
      
      if $?.success?
        return *fetch_files_in_hdfs(aliases).map {|lines| lines.map{|e| e.chomp("\n").split("\t", -1)}}        
      else
        raise "#{pig_out.join("\n")}Failed executing #{pig_execution}"
      end
    end
    
    private
    def fetch_files_in_hdfs(file_paths)
      mandy_config = @configs[:hadoop_config].nil? && "" || "-c #{@configs[:hadoop_config]}"
      return file_paths.map do |file_path|
        FileUtils.remove_file(file_path, true) if File.exists?(file_path)
        `mandy-get #{mandy_config} #{file_path} #{file_path}`
        `mandy-rm #{mandy_config} #{file_path}`
        File.open(file_path) {|file| file.readlines}
      end
    end
  end
end