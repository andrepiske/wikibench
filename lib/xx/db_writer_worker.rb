# frozen_string_literal: true

class DbWriterWorker
  def initialize(q, db_url, inserts_no)
    @q = q
    @inserts_no = inserts_no
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://127.0.0.1:7222/1")
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://192.168.1.192:7222/1")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@127.0.0.1:7111/postgres")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@192.168.1.192:7111/postgres")
    @driver = :postgres ; @pg_conn = Sequel.connect(db_url)
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
      @inserts_no.increment
    else
      raise "Invalid driver: #{@driver}"
    end
  end
end
