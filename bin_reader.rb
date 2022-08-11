# frozen_string_literal: true

require "stringio"

class BinDecoder
  def initialize(io)
    @io = io
  end

  def read_int32
    @io.read(4).unpack('>l').first
  end

  def read_uint32
    @io.read(4).unpack('>L').first
  end

  def read_int64
    @io.read(8).unpack('>q').first
  end

  def read_uint64
    @io.read(8).unpack('>Q').first
  end

  def read_byte
    @io.read(1).unpack('C').first
  end

  def read_string
    len = read_uint32
    return '' if len == 0
    @io.read(len).force_encoding('UTF-8')
  end
end

class BinReader
  ReadError = Class.new(StandardError)

  def initialize(io)
    @io = io
    @data = nil
  end

  def read_all(&block)
    read_file_header!

    while !@data.eof?
      read_single(&block)
    end
  end

  private

  def read_single
    hdr = @data.read(1).ord
    return if hdr == 0xD0 # end of block
    raise ReadError, "Expecting hdr 0xDD, got 0x#{hdr.to_s(16)}" if hdr != 0xDD

    blob = {}
    dec = BinDecoder.new(@data)
    blob[:page_id] = dec.read_int64
    blob[:revision_id] = dec.read_int64
    blob[:parent_id] = dec.read_int64
    blob[:ts] = dec.read_int64

    blob[:parent_id] = nil if blob[:parent_id] == -1
    blob[:ts] = Time.at(blob[:ts]).utc

    blob[:sha1] = dec.read_string
    blob[:revision_text] = dec.read_string
    blob[:is_redirect] = (dec.read_byte == 1)

    yield blob
  end

  def read_file_header!
    header = @io.read(2).unpack('CC')
    if header != [0x94, 0x23]
      raise ReadError, "Expected 0x94,0x23 header but didn't find it!"
    end
    data_length = @io.read(4).unpack('>L').first

    inflator = Zlib::Inflate.new
    @data = StringIO.new(inflator.inflate(@io.read(data_length)))
    @data.binmode

    # require "pry"; require "pry-byebug"; binding.pry

    if @data.read(1).ord != 0xFE
      raise ReadError, "Expected 0xFE block begin header"
    end
  end
end
