# frozen_string_literal: true

class Worker
  def initialize(q)
    @q = q
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://127.0.0.1:7222/1")
    # @driver = :redis; @redis_conn = Redis.new(url: "redis://192.168.1.192:7222/1")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@127.0.0.1:7111/postgres")
    # @driver = :postgres ; @pg_conn = Sequel.connect("postgres://postgres:root1337@192.168.1.192:7111/postgres")
    @drive = :postgres ; @pg_conn = Sequel.connect("postgres://wiki_app_usr:supersafe@192.168.1.192:5432/wiki_app")
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
