#!/usr/bin/env ruby
# frozen_string_literal: true
$:.unshift(File.expand_path("./lib", __dir__))

require "pg"
require "sequel"
require "nokogiri"
require "osto"
require "concurrent"
# require "zlib"
# require "pry"
# require "pry-byebug"

require "redis"
require "msgpack"

class PageRevision < Osto::Model
  col :id, :integer
  col :parentid, :integer
  col :timestamp, :timestamp
  col :sha1, :string
  col :text, :string
end

class WikiPage < Osto::Model
  col :title, :string
  col :id, :integer
  col :is_redirect, :boolean
  col :revisions, :array
end

class Worker
  def initialize(q)
    @q = q
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://127.0.0.1:7222/1")
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://192.168.1.192:7222/1")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@127.0.0.1:7111/postgres")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@192.168.1.192:7111/postgres")
    @driver = :postgres ; @pg_conn = Sequel.connect("postgres://wiki_app_usr:supersafe@192.168.1.192:5432/wiki_app")
  end

  def run_forever
    loop do
      data = @q.pop
      if data == nil
        sleep(0.1)
      else
        process_one(data)
      end
    end
  end

  def process_one(pg_content)
    # @pg_conn[:wiki_pages].insert(pg_content)
    case @driver
    when :redis
      rev_id = pg_content[:revision_id]
      data = MessagePack.pack(pg_content)
      @redis_conn.set("r:#{rev_id}", data)
    when :postgres
      @pg_conn[:wiki_pages].insert(pg_content)
    else
      raise "Invalid driver: #{@driver}"
    end
  end
end

class DbWriter
  def initialize(worker_count)
    @worker_count = worker_count
    @queue = Concurrent::Array.new
    # @sem = Concurrent::Semaphore.new(worker_count)
    # @sem.acquire(worker_count)
  end

  def insert(data)
    @queue << data
  end

  def queue_size
    @queue.length
  end

  def start
    @threads = (0...@worker_count).map do |index|
      Thread.new { w = Worker.new(@queue) ; w.run_forever }
    end
  end
end

# CREATE TABLE wiki_pages (
#   page_id INTEGER PRIMARY KEY,
#   revision_id INTEGER,
#   parent_id INTEGER,
#   ts TIMESTAMP WITHOUT TIME ZONE,
#   sha1 TEXT,
#   is_redirect BOOLEAN NOT NULL,
#   revision_text TEXT
# );
#
class WikiDoc < Nokogiri::XML::SAX::Document
  def initialize(*a)
    super(*a)
    @node_state = []
    @state_stack = []
    @writer = DbWriter.new(72)
    # @writer.start
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

    qs = @writer.queue_size
    if qs > 10000
      print("Queue too large (#{qs}), throttling ")

      while @writer.queue_size > 5000
        print(".")
        sleep(0.2)
      end
      puts("")
    end

    # if @num_pages % 1000 == 0
      time_now = Time.now.to_f * 1000.0
      if @last_ts
        diff = time_now - @last_ts
        if diff >= 10000.0
          pages_ps = 1000.0 * @cyc_pages / diff
          tot_ps = 1000.0 * @num_pages / (time_now - @first_page_ts)

          puts("%.2f pages per second (%.2f total) %06dp [q=%d]" % [pages_ps, tot_ps, @num_pages, @writer.queue_size])

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
      pg_content = {
        page_id: page.id,
        revision_id: rev.id,
        parent_id: rev.parentid,
        ts: rev.timestamp,
        sha1: rev.sha1,
        is_redirect: !!page.is_redirect,
        revision_text: rev.text
      }

      # @writer.insert(pg_content)
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

parser = Nokogiri::XML::SAX::Parser.new(WikiDoc.new)
puts "will start parsing"
parser.parse(File.open("/Volumes/AndreSG/enwiki-20190720-pages-articles.xml"))
