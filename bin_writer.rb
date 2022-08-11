# frozen_string_literal: true

require "zlib"

class BinEncoder
  def initialize
    @io = StringIO.new
    @io.binmode
  end

  def bytes
    @io.rewind
    @io.read
  end

  def write_int32(value)
    @io.write([value].pack('>l'))
  end

  def write_uint32(value)
    @io.write([value].pack('>L'))
  end

  def write_int64(value)
    @io.write([value].pack('>q'))
  end

  def write_uint64(value)
    @io.write([value].pack('>Q'))
  end

  def write_byte(value)
    @io.write([value].pack('C'))
  end

  def write_string(value)
    if value == nil
      @io.write("\x00".b)
      return
    end

    bin_value = value.b
    @io.write([ bin_value.length ].pack('>L'))
    @io.write(bin_value)
  end
end

class BinWriter
  PAGES_PER_BLOCK = 5_000 # no. of pages in a block

  def initialize(output_path)
    @output_path = output_path
    @buffer = nil
    @pages_in_block = 0
    @file_no = 1
  end

  def write_single(data)
    ensure_buffer!

    @buffer.write_byte(0xDD) # page / article

    # if data[:is_redirect]
    #   require "pry"; require "pry-byebug"; binding.pry
    # end

    @buffer.write_int64( data[:page_id] )
    @buffer.write_int64( data[:revision_id] )
    @buffer.write_int64( data[:parent_id] || -1 )
    @buffer.write_int64( data[:ts].utc.to_i )
    @buffer.write_string( data[:sha1] )
    @buffer.write_string( data[:revision_text] )
    @buffer.write_byte( data[:is_redirect] ? 1 : 0 )

    # @buffer.write_byte(0xDD) # page / article
    # @buffer.write_prefixed_int( data[:page_id] , 7)
    # @buffer.write_prefixed_int( data[:revision_id] , 7)
    # @buffer.write_prefixed_int( data[:parent_id] , 7)
    # @buffer.write_prefixed_int( data[:ts].utc.to_i , 7)
    # @buffer.write_string( data[:sha1] )
    # @buffer.write_string( data[:revision_text] )
    # @buffer.write_byte( data[:is_redirect] ? 1 : 0 )

    @pages_in_block += 1

    close_and_flush_block

    # require "pry"; require "pry-byebug"; binding.pry
  end

  private

  def ensure_buffer!
    return if @buffer

    # @buffer = BitEncoder.new
    @buffer = BinEncoder.new
    @buffer.write_byte(0xFE) # 0xFE = begin of block
  end

  def close_and_flush_block(force = false)
    return if !force && @pages_in_block < PAGES_PER_BLOCK

    @buffer.write_byte(0xD0) # 0xD0 = end of block

    deflator = Zlib::Deflate.new(5, Zlib::MAX_WBITS, 9)
    comp_data = deflator.deflate(@buffer.bytes, Zlib::FINISH)

    file_name = File.join(@output_path, ("%07d" % @file_no) + ".zwi")
    out_file = File.open(file_name, 'w')
    out_file.write([0x94, 0x23, comp_data.length].pack('>CCL'))
    out_file.write(comp_data)
    out_file.close

    puts "Wrote file #{file_name}"

    @file_no += 1
    @buffer = nil
    @pages_in_block = 0
  end
end
