#!/usr/bin/ruby
require 'yaml'
require 'logger'
require 'common'

module RTrans
    class Datamngr
        def initialize (config = {})
            RTransCommon.assert_required_keys(config, [:data_dir,
                :format_version, :size_per_index, :index_num_per_file, 
                :max_size_data_file, :data_blk_size])

            @data_dir = RTransCommon.check_type_return(
                config[:data_dir], "data_dir")
            @format_version = RTransCommon.check_type_return(
                config[:format_version], 1)
            @size_per_index = RTransCommon.check_type_return(
                config[:size_per_index], 1)
            @index_num_per_file = RTransCommon.check_type_return(
                config[:index_num_per_file], 1)
            @max_size_data_file = RTransCommon.check_type_return(
                config[:max_size_data_file], 1)
            @data_blk_size = RTransCommon.check_type_return(
                config[:data_blk_size], 1)

            init_check
        end

        def init_check
            #if not File.directory?(@data_dir)
            #    raise RTransCommon::RTransError::BadArguments,
            #        "#{@data_dir} is not a valid data dir"
            #end

            
            if File.exists?(RTransCommon::META_FILE)
                puts "exists!"
            else
                puts "not exists!"
            end
        end 
    end
end

hdlr = RTrans::Datamngr.new({:data_dir => "test", :format_version => 1,
    :size_per_index => 40, :index_num_per_file => 10000,
    :max_size_data_file => 20*1024*1024, :data_blk_size =>128})  
