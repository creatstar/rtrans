#!/usr/bin/ruby
require 'logger'
require 'common'
require 'zlib'

module RTrans
    class Datamngr
        def initialize (config = {})
            RTransCommon.assert_required_keys(config, [:data_dir, :format_version,
                :size_per_index, :index_num_per_file, :index_file_num_per_dir,
                :max_size_data_file, :data_blk_size])

            @data_dir = RTransCommon.check_type_return(config[:data_dir], "data_dir")
            @format_version = RTransCommon.check_type_return(config[:format_version], 1)
            @size_per_index = RTransCommon.check_type_return(config[:size_per_index], 1)
            @index_num_per_file = RTransCommon.check_type_return(config[:index_num_per_file], 1)
            @index_file_num_per_dir = RTransCommon.check_type_return(
                config[:index_file_num_per_dir], 1)
            @max_size_data_file = RTransCommon.check_type_return(config[:max_size_data_file], 1)
            @data_blk_size = RTransCommon.check_type_return(config[:data_blk_size], 1)
            @blk_num_per_file = @max_size_data_file / @data_blk_size
=begin
            puts @data_dir, @format_version, @size_per_index,
                @index_num_per_file, @max_size_data_file, @data_blk_size
=end                
            # do init check on the metadata file
            init_check
            
            # single thread writing, so only need one mark
            @cur_transid = get_biggest_transid
            @cur_dno, @cur_fno, @cur_offside_in_f = get_index_by_transid(@cur_transid)
        end

        def init_check
            # data_dir
            if not File.directory?(@data_dir)
                raise RTransCommon::RTransError::BadArguments,
                    "#{@data_dir} is not a valid data dir"
            end

            # metadata file
            @metadata_file =File.expand_path(RTransCommon::METADATA_FILE, @data_dir)
            if File.exists?(@metadata_file)
                # the meta file exists, so check the given parameters
                # metadata structure: [format_version, size_per_index,
                # index_num_per_file, max_size_data_file, data_blk_size]
                metadata_blk = File.open(@metadata_file).sysread(RTransCommon::METADATA_BLK_LEN)
                metadata_array = metadata_blk.unpack(RTransCommon::METADATA_PACK)
                RTransCommon.check_parameter("format_version", metadata_array[0], @format_version)
                RTransCommon.check_parameter("size_per_index", metadata_array[1], @size_per_index)
                RTransCommon.check_parameter("index_num_per_file", metadata_array[2], @index_num_per_file)
                RTransCommon.check_parameter("index_file_num_per_dir", metadata_array[3],
                    @index_file_num_per_dir)
                RTransCommon.check_parameter("max_size_data_file", metadata_array[4], @max_size_data_file)
                RTransCommon.check_parameter("data_blk_size", metadata_array[5], @data_blk_size)
            else
                # the meta not exists, so create the metadata file
                metadata_blk = [@format_version, @size_per_index, @index_num_per_file,
                    @index_file_num_per_dir, @max_size_data_file, @data_blk_size].pack(
                    RTransCommon::METADATA_PACK)
                f = File.open(@metadata_file, "w")
                f.syswrite(metadata_blk)
                # just sync the bin file to the disk
                f.sync
            end
        end

        def write_data(data)
            # prepare the data
            data_blk, data_len, crc_checksum = prepare_data(data)

            # note that we should write the data file first, then the data file
            # but we should check whether we need split index file first
            # write data file first
            data_dno, data_fno, data_offside = get_pos_for_data(@transid, data_len)
            d_name = File.expand_path(data_dno.to_s, @data_dir)
            f_name = File.expand_path(data_fno.to_s, d_name)
            f = File.open(f_name, "a").seek(data_offside*@data_blk_size, IO::SEEK_SET)
            data_head = [@transid+1, crc_checksum].pack(RTransCommon::DATA_HEAD_PACK)
            f.syswrite(data_head)
            f.syswrite(data_blk)
            f.sync if @need_sync
            f.close

            # write index file 
            , idx_dno, idx_fno, idx_offside = get_pos_by_transid(@transid+1)
            d_name = File.expand_path(idx_dno.to_s, @data_dir)
            f_name = File.expand_path(idx_fno.to_s, @data_dir)
            f = File.open(f_name, "a").seek(idx_offside*RTransCommon::INDEX_BLK_LEN, IO::SEEK_SET)
            idx_blk = [@transid+1, d_name, f_name, data_offside,
                data_len, crc_checksum, "0"].pack(RTransCommon::INDEX_PACK)
            f.syswrite(idx_blk)
            f.sync if @need_sync
            f.close

            @transid += 1
        end

        def prepare_data(data)
            data_blk = Marshal.dump(data)
            data_len = data_blk.size
            crc_checksum = Zlib.crc32(data_blk)
            [data_blk, data_len, crc_checksum]
        end

        def get_smallest_and_biggest_numeric_dir
            smallest_no = 2**32
            biggest_no = -1
            Dir.foreach(@data_dir) do |f|
                if f.to_i.to_s == f
                    f_no = f.to_i
                    d_name = File.expand_path(f, @data_dir)
                    if File.directory?(d_name)
                        smallest_no = (smallest_no > f_no)? f_no : smallest_no
                        biggest_no = (biggest_no < f_no)? f_no : biggest_no
                    end
                end        
            end
            smallest_no = (smallest_no == 2**32)? -1 : smallest_no
            biggest_no = (biggest_no == -1)? -1 : biggest_no
            [smallest_no, biggest_no]
        end

        def get_pos_by_transid(transid)
            return 0 if transid <= 0

            a = transid / (@index_num_per_file * @index_file_num_per_dir)
            b = transid % (@index_num_per_file * @index_file_num_per_dir)
            d_no = (b == 0)? a-1 : a

            transid_offside_in_d = transid - (@index_num_per_file * @index_file_num_per_dir * d_no)
            a = transid_offside_in_d / @index_num_per_file
            b = transid_offside_in_d % @index_num_per_file
            f_no = (b == 0)? a-1 : a

            blk_offside_in_f = transid_offside_in_d - (f_no * @index_num_per_file) - 1 
           
            [transid, d_no, f_no, blk_offside_in_f]
        end

        def get_index_by_pos(dno, fno, offside_in_f)
            idx_file = File.expand_path(dno.to_s, @data_dir)
            idx_file = File.expand_path(fno, idx_file)
            
            idx_blk = File.open(idx_file).sysread(RTransCommon::INDEX_BLK_LEN)
            idx_array = idx_blk.unpack(RTransCommon::INDEX_PACK)
            idx_array
        end

        def get_pos_for_data(transid, data_len)
            transid_old, d_no_old, f_no_old, offside_old = get_pos_by_transid(transid)
            transid_new, d_no_new, f_no_new, offside_new = get_pos_by_transid(transid+1)

            if d_no_old == d_no_new
                # no need to create dir
                idx_array = get_index_by_pos(get_pos_by_transid(transid))
                # check whether we should split the data file
                blk_offside = (data_len/@data_blk_size).ceil + (idx_array[4]/@data_blk_size).ceil
                if (idx_array[3] + blk_offside) >= @blk_num_per_file
                    # need to split the data file
                    data_dno = idx_array[1]
                    data_fno = idx_array[2] + 1
                    data_offside = 0
                    return [data_dno, data_fno, data_offside]
                else
                    # need not split the data file
                    data_dno = idx_array[1]
                    data_fno = idx_array[2]
                    data_offside = idx_array[3] + (idx_array[4]/@data_blk_size).ceil
                    return [data_dno, data_fno, data_offside]
                end
            else
                # need to create dir
                data_dno = d_no_new
                data_fno = 0
                data_offside = 0

                d_name = File.expand_path(data_dno.to_s, @data_dir)
                Dir.mkdir(d_name)
                return [data_dno, data_fno, data_offside]
            end
        end

        def get_smallest_transid
            smallest_dir_no, tmp = get_smallest_and_biggest_numeric_dir
            return -1 if 0 > smallest_dir_no

            d_name = File.expand_path(smallest_dir_no.to_s, @data_dir)
            smallest_file_no = @index_file_num_per_dir
            Dir.foreach(d_name) do |f|
                if d_name =~ /index.(\d+)/
                    smallest_file_no = (smallest_file_no > $~[1].to_i)? $~[1].to_i : smallest_file_no 
                end
            end
            
            if smallest_file_no == @index_file_num_per_dir
                if smallest_dir_no > 0 
                    # something strange happened
                    raise RTransCommon::RTransError::InternalError,
                        "no smallest transid to read from"
                else
                    # we have dir 0 but no file
                    return 0
                end
            end
            
            (@index_num_per_file * @index_file_num_per_dir * smallest_dir_no
                + @index_num_per_file * smallest_file_no)
        end

        def get_biggest_transid
            tmp, biggest_dir_no = get_smallest_and_biggest_numeric_dir
            return -1 if 0 > biggest_dir_no

            d_name = File.expand_path(biggest_dir_no.to_s, @data_dir)
            biggest_file_no = -1
            Dir.foreach(d_name) do |f|
                if d_name =~ /index.(\d+)/
                    biggest_file_no = (biggest_file_no < $~[1].to_i)? $~[1].to_i : biggest_file_no
                end
            end

            if biggest_file_no == -1
                if biggest_dir_no > 0
                    # check out the dir 1 smaller
                    d_name = File.expand_path((biggest_dir_no - 1).to_s, @data_dir)
                    biggest_file_no = -1
                    Dir.foreach(d_name) do |f|
                        if d_name =~ /index.(\d+)/
                            biggest_file_no = (biggest_file_no < $~[1].to_i)? $~[1].to_i : biggest_file_no
                        end
                    end
                    
                    if biggest_file_no != @index_file_num_per_dir -1
                        # something strange happened
                        raise RTransCommon::RTransError::InternalError,
                            "we have #{biggest_dir_no} dir without file, and the biggest_file_no\
                             in #{biggest_dir_no-1} dir is #{biggest_file_no} (should be \
                             #{@index_file_num_per_dir-1})"
                    end

                    f_name = File.expand_path(biggest_file_no.to_s, d_name)
                    if File.size(f_name) != (@index_num_per_file * RTransCommon::METADATA_BLK_LEN)
                        # something strange happened
                        raise RTransCommon::RTransError::InternalError,
                            "we have #{biggest_dir_no} dir without file, and the biggest_file_no\
                            in #{biggest_file_no-1} dir is #{biggest_file_no}, the size is\
                            #{File.size(f_name)} (should be #{@index_num_per_file * RTransCommon::METADATA_BLK_LEN})"
                    end

                    return (@index_num_per_file * @index_file_num_per_dir * biggest_dir_no - 1)
                else
                    # we have dir 0 but no file
                    return 0
                end
            end
            
            f_name = File.expand_path(biggest_file_no.to_s, d_name)
            idx_no = File.size(f_name) / RTransCommon::METADATA_BLK_LEN
            (@index_num_per_file * @index_file_num_per_dir * biggest_dir_no
                + @index_num_per_file * (smallest_file_no -1) + idx_no)
        end
    end
end

hdlr = RTrans::Datamngr.new({:data_dir => "test", :format_version => 1,
    :size_per_index => 40, :index_num_per_file => 10000, :index_file_num_per_dir => 64,
    :max_size_data_file => 20*1024*1024, :data_blk_size =>128}) 
#puts hdlr.get_smallest_transid
#puts hdlr.get_biggest_transid
#hdlr.get_index_by_transid(0)
#hdlr.get_index_by_transid(1)
#hdlr.get_index_by_transid(1000)
hdlr.get_index_by_transid(10001)
hdlr.get_index_by_transid(10002)
hdlr.get_index_by_transid(10000*64)
