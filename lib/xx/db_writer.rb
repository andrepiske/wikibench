# frozen_string_literal: true

class DbWriter
  def initialize(worker_count, db_url, data_logger)
    @data_logger = data_logger
    @db_url = db_url
    @worker_count = worker_count
    @queue = Concurrent::Array.new
    @sem = Concurrent::Semaphore.new(worker_count)
    @sem.acquire(worker_count)

    @inserted_pages = Concurrent::AtomicFixnum.new(0)
  end

  def insert(data)
    @first_page_ts ||= Time.now.to_f * 1000.0

    if true # @num_pages % 1000 == 0
      time_now = Time.now.to_f * 1000.0
      if @last_ts
        total_pages = @inserted_pages.value
        cycle_pages = total_pages - @last_cycle_pages
        @last_cycle_pages = total_pages

        diff = time_now - @last_ts
        if diff >= 500.0
          pages_ps = 1000.0 * cycle_pages / diff
          tot_ps = 1000.0 * total_pages / (time_now - @first_page_ts)

          puts("%.2f pages per second (%.2f total) %06dp [q=%d]" % [pages_ps, tot_ps, total_pages, @queue.length])

          @data_logger.log_point({
            pages_ps: pages_ps,
            tot_ps: tot_ps,
            total_pages: total_pages,
            cycle_pages: cycle_pages,
            queue_length: @queue.length
          })

          @last_ts = time_now
        end
      else
        @last_ts = time_now
        @last_cycle_pages = 0
      end
    end

    while @queue.length > 10000
      # puts "Queue too long (#{@queue.length}, inserted=#{@inserted_pages.value}), waiting..."
      sleep 0.2
    end

    @queue << data
    # puts("insert #{data}")
  end

  def start
    @threads = (0...@worker_count).map do |index|
      Thread.new { w = DbWriterWorker.new(@queue, @db_url, @inserted_pages) ; w.run_forever }
    end
  end
end
