require "dataframe/version"
require "dataframe/row"
require 'pry'

module Dataframe
  class Table
    include Enumerable

    attr_accessor :raw_data, :chain

    def initialize(data, chain = nil)
      self.raw_data = data
      self.chain = chain || Dataframe::Table.noop
    end

    def enumerate
      Enumerator.new do |y|
        raw_data.each do |row|
          # binding.pry
          y.yield(chain.call(Dataframe::Row(row)))
        end
      end
    end

    # actually compute what's defined - not sure this is it really
    def each(&block) #what about that block???
      self.enumerate.each do |row|
        yield row
      end
    end

    # add computed column, return new data frame
    def compute(column_name, &block)
      new_chain = Proc.new do |row|
        row[column_name.to_sym] = block.call(chain.call(row))
        row
      end
      Dataframe::Table.new(self.raw_data, new_chain)
    end

    # return multiple rows
    def reshape(&block)
      new_collection = Enumerator.new do |yielder| #possible wrap yielder (something like collection.emit)
        self.each {|row| block.call(row, yielder)}
        block.call(nil, yielder)
      end
      return Dataframe::Table.new(new_collection, Dataframe::Table.noop)
    end

    # from long to wide:
    # accumulate value_field values,
    # index by per_field values,
    # format [[field_name, field_value], ....] <-- include these
    # one row per by_field values
    def collect(by_field, per_field, value_field, per_values = nil, options = {})
      options = {:key_prefix => ''}.merge(options)
      by_value = nil
      outrow = {}
      self.reshape do |row, yielder|
        if row
          if row[by_field] != by_value #start next row
            if by_value #emit previous
              yielder.yield(Dataframe::Row(outrow))
            end
            outrow = {}
            if per_values # support placeholders
              per_values.each do |v|
                outrow[options[:key_prefix] + v.to_s] = nil
              end
            end
            by_value = row[by_field]
            outrow[by_field] = by_value
          end
          outrow[options[:key_prefix] + row[per_field].to_s] = row[value_field] if per_values.nil? || per_values.include?(row[per_field])
        else #get the last row
          yielder.yield(Dataframe::Row(outrow))
        end
      end
    end

    def select(&block)
      new_collection = Enumerator.new do |yielder|
        self.each {|row| yielder.yield row if block.call(row)}
      end
      return Dataframe::Table.new(new_collection, Dataframe::Table.noop)
    end

    def pick(*names)
      if names.first.is_a?(Hash) && names.count == 1
        hnames = names.first
      else
        hnames = Hash[*(names.map {|n| [n, nil]}.flatten)]
      end
      new_chain = Proc.new do |row|
        hn = hnames.dup
        crow = chain.call(row)
        # p crow
        hn.each {|k,v| hn[k] = crow[k]}
        Dataframe.Row(hn)
      end
      Dataframe::Table.new(self.raw_data, new_chain)
    end

    # arg: :old_name => :new_name, :other_old_name => :other_new_name
    # renames a column
    def rename(*mapping)
      new_chain = Proc.new do |row|
        crow = chain.call(row).dup
        old_new = Hash[*mapping]
        old_new.each do |old_key, new_key|
          crow[new_key] = crow.delete(old_key)
        end
        crow
      end
      Dataframe::Table.new(self.raw_data, new_chain)
    end

    # arg: :column_name => :default_value, :other_column_name => :other_default_value
    # replace nil values with indicated default
    def default(*mapping)
      new_chain = Proc.new do |row|
        crow = chain.call(row).dup
        key_default = Hash[*mapping]
        key_default.each do |key, default|
          crow[key] = default if crow[key].nil?
        end
        crow
      end
      Dataframe::Table.new(self.raw_data, new_chain)
    end

    # arg :key => values_array
    # require presence of each value or insert blank row
    # add missing rows
    def fill(*mapping)
      # exception if mapping length other than to
      throw Dataframe::ArgumentError.new('fill(:key => value_array) required') unless
        mapping.length == 2 && mapping.last.is_a?(Array)

      key = mapping.first
      values = {}
      mapping.last.each do |value|
        values[value] = true
      end
      new_collection = Enumerator.new do |yielder|
        self.each {|row| values.delete(row[key]); yielder.yield row}
        values.keys.each do |value|
          row = {}
          # TODO - ensure a fixed column list....
          row[key] = value
          yielder.yield row
        end
      end
      return Dataframe::Table.new(new_collection, Dataframe::Table.noop)
    end

    # quite nasty - scans both collections.
    # some day I should build a lookup abstraction which could be optimized
    # against storage
    def from(other_collection, join_condition, columns, prefix = 'other_')
      other_index = {}
      other_collection.pick(join_condition.values).each_with_index do |row, i|
        other_index[row.values] = row
      end
      new_chain = Proc.new do |row|
        crow = chain.call(row)
        insertable = other_collection[join_condition.map {|k,v| crow[k]}]
        columns.each {|k| crow[(other_ + k.to_s).to_sym] = insertable[k]} if insertable
        Dataframe.Row(crow)
      end
      Dataframe::Table.new(self.raw_data, new_chain)
    end

    # arg: :column_name => column_rule
    # nil out values that don't match given criteria
    # can't build until I've figured out how to formulate the rules
    def normalize_values(*rules)
      # TODO
    end


    # TODO options hash?
    def merge(other_collection, merge_keys, options = {:merge_type => :ignore})
      # :ignore, :append, :replace, :fail
      # run througn this, build index - append if desired
    end

    # merge rows identified by key
    def join(right_collection, key, joined_key = nil, &merge_plan)
      unless merge_plan
        merge_plan = Proc.new  do |a,b|
          if b && a
            b.merge(a)
          else
            nil
          end
        end
      end
      unless joined_key
        joined_key = key
      end
      index = {}
      right_collection.each {|row| index[row[joined_key]] = row}
      joined_collection = Enumerator.new do |join|
        self.each do |row|
          res = merge_plan.call(row, index[row[key]])
          join.yield res if res
        end
      end
      return Dataframe::Table.new(joined_collection, Dataframe::Table.noop)
    end

    def all
      self.enumerate.map {|r| r}
    end

    ## radicals
    # def combine(radical)
    #   new_chain = Proc.new do |row|
    #
    #   end
    # end
    def self.noop
      return Proc.new {|x| x}
    end
  end
end
