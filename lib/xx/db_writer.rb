# frozen_string_literal: true

class DbWriter
  def initialize(worker_count)
    # @worker_count = worker_count
    # @queue = Concurrent::Array.new
    # @sem = Concurrent::Semaphore.new(worker_count)
    # @sem.acquire(worker_count)
    @bin_writer = BinWriter.new("/Volumes/Bento/pages")
  end

  def insert(data)
    # @queue << data
    # puts("insert #{data}")

    @bin_writer.write_single(data)
  end

  def queue_size
    1
    # @queue.length
  end

  def start
    # @threads = (0...@worker_count).map do |index|
    #   Thread.new { w = Worker.new(@queue) ; w.run_forever }
    # end
  end
end
