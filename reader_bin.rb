#!/usr/bin/env ruby
# frozen_string_literal: true

$:.unshift(File.expand_path("./lib", __dir__))

require "time"
require "date"
require "zlib"
require "osto"
require_relative './models'
require_relative './bin_reader'

class ReaderBin
  def initialize(files_path)
    @files_path = files_path
  end

  def run
    load_file_list

    @file_list.each do |file_name|
      load_single_file(File.join(@files_path, file_name))
    end
  end

  private

  def load_single_file(full_path)
    file_no = File.basename(full_path).to_i
    puts "Reading file #{file_no}"

    pages = 0

    File.open(full_path) do |file|
      BinReader.new(file).read_all do |page|
        pages += 1
      end
    end

    puts("File #{file_no}, read #{pages} pages")
  end

  def load_file_list
    @min_no = 2 ** 32
    @max_no = -1

    @file_list = Dir.glob("#{@files_path}/*.zwi").map do |path|
      File.basename(path).tap do |name|
        no = name.to_i
        @min_no = no if no < @min_no
        @max_no = no if no > @max_no
      end
    end

    puts("Found #{@file_list.length} files. Start=#{@min_no}, End=#{@max_no}")
  end
end

puts "will start reading"
ReaderBin.new("/Volumes/Bento/pages").run
