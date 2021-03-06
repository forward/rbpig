require 'fileutils'
require 'rexml/document'

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
      {:hadoop_config => nil}.merge(configs || {})
    end
    
    def pig_classpath(configs)
      classpath = [
        "#{File.join(File.dirname(__FILE__), %w[.. java dist porkchop.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive hive-metastore-0.7.0-CDH3B4.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive thrift-0.5.0.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib hive thrift-fb303-0.5.0.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib jsp-2.1.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib log4j-1.2.16.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib slf4j-api-1.6.1.jar])}",
        "#{File.join(File.dirname(__FILE__), %w[.. java lib slf4j-log4j12-1.6.1.jar])}"
      ]
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
      
      execute("#{RBPig.executable(@configs)} -f #{pig_script_path} 2>&1")
      return *fetch_files_in_hdfs(aliases).map {|lines| lines.map{|e| e.chomp("\n").split("\t", -1)}}        
    end
    
    private
    def execute(execution)
      out = []
      IO.popen(execution) do |stdout|
        puts execution
        until stdout.eof? do
          out << stdout.gets
          puts out.last
        end
      end

      raise "#{out.join("\n")}Failed executing #{execution}" unless $?.success?
    end
    
    def fetch_files_in_hdfs(file_paths)
      mandy_config = @configs[:hadoop_config].nil? && "" || "-c #{@configs[:hadoop_config]}"
      return file_paths.map do |file_path|
        FileUtils.remove_file(file_path, true) if File.exists?(file_path)
        execute("mandy-get #{mandy_config} #{file_path} #{file_path} 2>&1")
        execute("mandy-rm #{mandy_config} #{file_path} 2>&1")
        File.open(file_path) {|file| file.readlines}
      end
    end
  end
end