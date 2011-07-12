require 'test/unit'
  
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

    #def test_write_read1
    #    # no sync, no compress
    #    system("mkdir -p tmp")
    #    hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
    #        :size_per_index => 40, :index_num_per_file => 1000, :index_file_num_per_dir => 64,
    #        :max_size_data_file => 20*1024*1024, :data_blk_size =>64}) 
    #    start_time = Time.new
    #    1.upto(200000) do |i|
    #        hdlr.write_data("this is the #{i}th data!")
    #    end
    #    end_time = Time.new
    #    
    #    printf("test_write_read1 write speed is: %f\n", 200000.to_f/(end_time-start_time))
    #    system("rm -rf tmp")
    #end

    #def test_write_read2
    #    # sync, no compress
    #    system("mkdir -p tmp")
    #    hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
    #        :size_per_index => 40, :index_num_per_file => 1000, :index_file_num_per_dir => 64,
    #        :max_size_data_file => 20*1024*1024, :data_blk_size =>64, :need_sync=>1}) 
    #    start_time = Time.new
    #    1.upto(200000) do |i|
    #        hdlr.write_data("this is the #{i}th data!")
    #    end
    #    end_time = Time.new

    #    printf("test_write_read1 write speed is: %f\n", 200000.to_f/(end_time-start_time))
    #    system("rm -rf tmp")
    #end

    def test_real_log
        # test with the real production log
        lines = []
        f = File.open("log/production.log.s")
        f.each_line do |line|
            line.strip!
            lines << line if line.size > 0
        end
        
        p "start writing!"
        hdlr = RTrans::Datamngr.new({:data_dir => "tmp", :format_version => 1,
            :size_per_index => 40, :index_num_per_file => 10000, :index_file_num_per_dir => 64,
            :max_size_data_file => 20*1024*1024, :data_blk_size =>64, :need_sync => 1}) 
        start_time = Time.new

        p "total lines: #{lines.size}"
        i = 0
        total_len = 0
        lines.each do |line|
            hdlr.write_data(line)
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
            assert_equal(res, lines[i])
        end
    end
end
