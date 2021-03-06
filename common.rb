#!/usr/bin/ruby

module RTransCommon

    METADATA_BLK_LEN = 26
    METADATA_FILE = "metadata"
    METADATA_PACK = "S1L1L1L1Q1L1S1"
    METADATA_HASH = {:FORMAT_VERSION => 0,
                     :SIZE_PER_INDEX => 1,
                     :INDEX_NUM_PER_FILE => 2,
                     :INDEX_FILE_NUM_PER_DIR =>3,
                     :MAX_SIZE_DATA_FILE => 4,
                     :DATA_BLK_SIZE => 5,
                     :NEED_COMPRESS => 6}

    INDEX_BLK_LEN = 40
    INDEX_PREFIX = "index."
    INDEX_PACK = "Q1L1L1Q1L1L1Q1"
    INDEX_HASH = {:TRANSID => 0,
                  :TYPE_NO => 1,
                  :DIR_NO => 2,
                  :FILE_NO => 3,
                  :DATA_BLK_NO => 4,
                  :DATA_LEN => 5,
                  :CHECK_SUM => 6}

    DATA_HEAD_LEN = 12
    DATA_PREFIX = "data."
    DATA_HEAD_PACK = "Q1L1"
    DATA_HASH = {:TRANSID => 0,
                 :CHECK_SUM => 1}


    module RTransError
        class InternalError < StandardError; end

        class BadArguments < StandardError; end
        class BadType < StandardError; end
        class BadChecksum < StandardError; end
    end
    
    def self.check_parameter(param_name, param_val, expect)
        unless (param_val == expect)
            raise RTransError::BadArguments,
            "#{param_name} is #{param_val}, not equal to #{expect}"
        end
    end
    
    def self.check_type_return(obj, target_type)
        unless (obj.is_a? target_type.class)
            raise RTransError::BadType,
            "#{obj} is not a #{target_type.class}"
        end
        obj
    end

    def self.assert_supported_keys(args, supported)
        unless (args.keys - supported).empty?
            raise RTransError::BadArguments,  
            "Supported arguments are: #{supported.inspect}, but arguments #{args.keys.inspect} were supplied instead"
        end
    end

    def self.assert_required_keys(args, required)
        unless (required - args.keys).empty?
            raise RTransError::BadArguments,
            "Required arguments are: #{required.inspect}, but only the arguments #{args.keys.inspect} were supplied."
        end 
    end 

    # some common file and directory functions

end
