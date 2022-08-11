#!/usr/bin/env ruby
# frozen_string_literal: true
$:.unshift(File.expand_path("../lib", __dir__))
require "xx"
require "slop"

class WikiDoc < Nokogiri::XML::SAX::Document
  def initialize(*a)
    @options = a.shift
    super(*a)
    @node_state = []
    @state_stack = []
    @bin_writer = BinWriter.new(@options[:out_dir])
  end

  def finish_bin_writer
    @bin_writer.flush
  end

  def state
    @state_stack.last
  end

  def start_element(name, attributes = [])
    if state != nil
      send("process_#{state}", name, attributes)
    elsif name == "page" && @node_state.length == 1 && @node_state[0] == "mediawiki"
      @state_stack << :page
      @current_page = WikiPage.new
    else
      @node_state << name
    end
  end

  def characters(value)
    if state == :value
      @current_value << value
    end
  end

  def end_element(name)
    if state != nil
      send("finish_#{state}", name)
    else
      @node_state.pop
    end
  end

  private

  def process_page(name, attributes)
    if name == "revision"
      @state_stack << :revision
      @current_revision = PageRevision.new
    else
      if WikiPage.has_column?(name)
        begin_value(@current_page, name)
      else
        begin_ignore
      end
    end
  end

  def finish_page(name)
    page = @current_page
    @current_page = nil
    @state_stack.pop

    @first_page_ts ||= Time.now.to_f * 1000.0

    @num_pages ||= 0
    @cyc_pages ||= 0
    @num_pages += 1
    @cyc_pages += 1

    # if @num_pages % 1000 == 0
      time_now = Time.now.to_f * 1000.0
      if @last_ts
        diff = time_now - @last_ts
        if diff >= 10000.0
          pages_ps = 1000.0 * @cyc_pages / diff
          tot_ps = 1000.0 * @num_pages / (time_now - @first_page_ts)

          puts("%.2f pages per second (%.2f total) %06dp" % [pages_ps, tot_ps, @num_pages])

          @cyc_pages = 0
          @last_ts = time_now
        end
      else
        @last_ts = time_now
        @cyc_pages = 0
      end
    # end

    prefix = "[%06d] " % @num_pages

    revs = page.revisions.length
    if revs > 1
      puts "#{prefix}got a page: #{page.title} (#{page.revisions.length} revisions)"
    elsif revs == 0
      puts "#{prefix}got a redirect: #{page.title}"
    else
      # puts "#{prefix}got a page: #{page.title}"

      rev = page.revisions.first
      # pg_content = {
      #   page_id: page.id,
      #   revision_id: rev.id,
      #   parent_id: rev.parentid,
      #   ts: rev.timestamp,
      #   sha1: rev.sha1,
      #   is_redirect: !!page.is_redirect,
      #   revision_text: rev.text
      # }

      @bin_writer.write_single(page, rev)
    end
  end

  def process_revision(name, attributes)
    if PageRevision.has_column?(name)
      begin_value(@current_revision, name)
    else
      begin_ignore
    end
  end

  def finish_revision(name)
    @current_page.revisions << @current_revision
    @current_revision = nil
    @state_stack.pop
  end

  ### IGNORE

  def begin_ignore
    @state_stack << :ignore
    @ignore_nodes = []
  end

  def process_ignore(name, attributes)
    @ignore_nodes << name
  end

  def finish_ignore(name)
    if @ignore_nodes.empty?
      @state_stack.pop
      @ignore_nodes = nil
    else
      @ignore_nodes.pop
    end
  end

  ### VALUE

  def begin_value(obj, attr_name)
    @current_value = []
    @value_nodes = []
    @state_stack << :value
    @value_dst = [obj, attr_name]
  end

  def process_value(name, attributes)
    raise "Values can't contain nodes inside: #{name}"
  end

  def finish_value(name)
    obj, attr_name = @value_dst
    obj.set_lax(attr_name, @current_value.join)
    @current_value = nil
    @value_nodes = nil
    @value_dst = nil
    @state_stack.pop
  end
end

options = Slop.parse do |o|
  o.string "--input", "Input XML file (.xml.bz2 file)", required: true
  o.string "--out-dir", "Output directory for binary files", required: true
end

wiki_doc = WikiDoc.new(options)
parser = Nokogiri::XML::SAX::Parser.new(wiki_doc)
puts "will start parsing"
parser.parse(Bzip2::FFI::Reader.open(options[:input]))
wiki_doc.finish_bin_writer
