module RBPig
  class Dataset
    class << self
      HIVE_CONFIG = {:database => "default", :database_root => "/user/hive/warehouse"}
      
      def hive(table_name, storage_type = :text_file, config = {})
        raise "storage type other than :text_file is not supported." unless storage_type == :text_file
        
        config = HIVE_CONFIG.merge(:field_separator => "\\t").merge(config)
        new("#{table_name} = LOAD '#{config[:database_root]}/#{table_name}' USING HiveTableLoader('#{config[:field_separator]}', '#{config[:database]}');")
      end
    end
    
    def to_s
      @load_script
    end
    
    private
    def initialize(load_script)
      @load_script = load_script
    end
  end
end