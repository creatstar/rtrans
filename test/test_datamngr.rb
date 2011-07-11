require 'test/unit'
  
$LOAD_PATH.unshift(File.expand_path('..', File.dirname(__FILE__))).uniq!
require 'datamngr'

class TestRTrans < Test::Unit::TestCase
    def test_get_pos_by_transid
        system("mkdir -p tmp")
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
        system("rm -rf tmp")
    end
end
