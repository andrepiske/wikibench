require "time"
require "date"

module Osto
end

class Osto::Database
end

class Osto::ColumnDefinition
  attr_accessor :name # Symbol
  attr_accessor :type # Symbol

  def initialize(name, type)
    @name = name
    @type = type
  end
end

TYPE_MATCHING = {
  string: [String],
  integer: [Integer],
  boolean: [TrueClass, FalseClass],
  timestamp: [Time, DateTime, Date],
  array: [Array],
}.freeze

class Osto::Model
  def initialize(**args)
    _initialize_values(args || {})
  end

  # Set the value, performing necessary conversions to accomodate the type
  def set_lax(name, lax_value)
    typed_value = _accomodate_value_to_type(self.class.get_column(name).type, lax_value)
    self.send("#{name}=", typed_value)
  end

  def _accomodate_value_to_type(type, value)
    return nil if value == nil
    case type
    when :string
      value.to_s
    when :integer
      Integer(value)
    when :boolean
      if String === value
        vd = value.downcase
        return true if vd == "true" || vd == "yes"
        return false if vd == "false" || vd == "no"
        raise "Can't accomodate string '#{value}' into boolean"
      elsif Integer === value
        return value != 0
      end
    when :timestamp
      if String === value
        Time.parse(value)
      else
        value.to_time
      end
    when :array
      value.to_a
    else
      raise "Can't accomodate value '#{value}' of type #{value.class}"
    end
  end

  def set_value(name, value)
    defn = self.class.get_column(name)
    if value != nil
      match_types = TYPE_MATCHING.fetch(defn.type)
      unless match_types.any? { |t| value.class == t }
        raise "Invalid value '#{value}' with type #{value.class} for column #{defn.name} in model #{self.class}"
      end
    end

    self.instance_variable_set("@#{name}", value)
  end

  def self.col(name, type)
    n = name.to_sym
    @cols ||= {}
    @cols[n] = Osto::ColumnDefinition.new(n, type.to_sym)

    define_method("#{n}=".to_sym) do |value|
      self.set_value(n, value)
    end

    define_method(n) do
      self.instance_variable_get("@#{n}")
    end
  end

  def self.columns
    @cols
  end

  def self.get_column(name)
    @cols[name.to_sym]
  end

  def self.has_column?(name)
    @cols.key?(name.to_sym)
  end

  private

  def _initialize_values(values)
    self.class.columns.each do |name, defn|
      init_value = if defn.type == :array
        []
      else
        nil
      end
      self.set_value(name, values.fetch(name.to_sym, init_value))
    end
  end
end
