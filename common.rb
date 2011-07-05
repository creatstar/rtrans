#!/usr/bin/ruby
#
#

module RTransCommon
    METADATA_FILE = "metadata"

    module RTransError
        class BadArguments < StandardError; end
        class BadType < StandardError; end
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
end
