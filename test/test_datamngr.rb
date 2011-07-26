require 'test/unit'
require 'zlib'
  
$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__))).uniq!
require 'datamngr'

class TestRTrans < Test::Unit::TestCase
    def setup
        system("rm -rf tmp")
        system("mkdir -p tmp")
    end
    
    def teardown
        system("rm -rf tmp")
    end 

    def test_get_pos_by_transid
        # test the correctness of calculating the directory and file corresponding to
        # the given transid
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 100, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>128}) 
        expected = hdlr.get_pos_by_transid(-1)
        assert_equal(expected, [-1,-1,-1])
        expected = hdlr.get_pos_by_transid(0)
        assert_equal(expected, [-1,-1,-1])
        expected = hdlr.get_pos_by_transid(1)
        assert_equal(expected, [0,0,0])
        expected = hdlr.get_pos_by_transid(100)
        assert_equal(expected, [0,0,99])
        expected = hdlr.get_pos_by_transid(101)
        assert_equal(expected, [0,1,0])
        expected = hdlr.get_pos_by_transid(6400)
        assert_equal(expected, [0,63,99])
        expected = hdlr.get_pos_by_transid(6401)
        assert_equal(expected, [1,0,0])
    end
    
    def test_write_read0
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 1000, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>64}) 
        # write a piece of data
        raw_data = "Started POST \"/app_logs\" for 69.171.176.5 at Sun Jul 03 06:33:06 +0800 2011"
        hdlr.write_data(0, raw_data)
        
        # check the index
        File.open("./tmp/0/index.0") do |f|
            idx_blk = f.sysread(RTransCommon::INDEX_BLK_LEN)
            idx_array = idx_blk.unpack(RTransCommon::INDEX_PACK)
            assert_equal(1, idx_array[RTransCommon::INDEX_HASH[:TRANSID]])
            assert_equal(0, idx_array[RTransCommon::INDEX_HASH[:TYPE_NO]])
            assert_equal(0, idx_array[RTransCommon::INDEX_HASH[:DIR_NO]])
            assert_equal(0, idx_array[RTransCommon::INDEX_HASH[:FILE_NO]])
            assert_equal(0, idx_array[RTransCommon::INDEX_HASH[:DATA_BLK_NO]])

            data_len = idx_array[RTransCommon::INDEX_HASH[:DATA_LEN]]
            File.open("./tmp/0/data.0") do |d|
                d.seek(RTransCommon::DATA_HEAD_LEN, IO::SEEK_SET)
                data_blk = d.sysread(data_len)
                checksum = idx_array[RTransCommon::INDEX_HASH[:CHECK_SUM]]
                assert_equal(Zlib.crc32(data_blk), checksum)

                data = Marshal.load(data_blk)
                assert_equal(raw_data, data)
            end
        end  
    end

    def test_write_read1
        # no sync, no compress
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 1000, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>64}) 
        start_time = Time.new
        write_count = 20000
        1.upto(write_count) do |i|
            # use the default type_no 0
            hdlr.write_data(0, "this is the #{i}th data!")
        end
        end_time = Time.new
        
        printf("test_write_read1 write speed is: %f\n", write_count.to_f/(end_time-start_time))
    end

    def test_write_read2
        # sync, no compress
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 1000, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>64, :need_sync=>1}) 
        start_time = Time.new
        write_count = 20000
        1.upto(write_count) do |i|
            # use the default type_no 0
            hdlr.write_data(0, "this is the #{i}th data!")
        end
        end_time = Time.new

        printf("test_write_read2 write speed is: %f\n", write_count.to_f/(end_time-start_time))
    end

    def test_real_log
        # test with the real production log
        lines = []
        f = File.open("log/production.log.small")
        f.each_line do |line|
            line.strip!
            lines << line if line.size > 0
        end
        
        #p "start writing!"
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 10000, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>64, :need_sync => 1}) 
        start_time = Time.new

        #p "total lines: #{lines.size}"
        i = 0
        total_len = 0
        lines.each do |line|
            hdlr.write_data(0, line)
            #p line
            i += 1
            total_len += line.size
            if i % 10000 == 0
                end_time = Time.new
                printf("test_real_log speed is: %f\n", i.to_f/(end_time-start_time))
                printf("average log len: %f\n", total_len.to_f/i)
            end
        end
        end_time = Time.new
        printf("test_real_log speed is: %f\n", lines.size.to_f/(end_time-start_time))

        # read and check
        0.upto(lines.size-1) do |i|
            res = hdlr.read_data(i+1)
            #p res, lines[i]
            assert_equal(res, lines[i], "the #{i}th line")
        end
    end
end
