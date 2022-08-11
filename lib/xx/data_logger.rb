# frozen_string_literal: true

class DataLogger
  def initialize(file_name)
    @output = File.open(file_name, 'w')
  end

  def log_point(data)
    date = Time.now.utc.iso8601(3)
    text = "[#{date}] #{JSON.dump(data)}"
    @output.puts(text)
    @output.flush
  end
end
