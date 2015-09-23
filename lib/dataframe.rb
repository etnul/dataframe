require "dataframe/version"
require "dataframe/row"
require "dataframe/exception"
require 'pry'

# TODO - input validation and throw argument errors

module Dataframe
  class Table
    include Enumerable

    # TODO - just effing refactor!
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

    def map(*args, &block)
      self.all.map(*args, &block)
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
    # example input
    # DATE        ITEM      PRICE
    # 2010-01-01  shoes     23.45
    # 2010-01-01  socks     5.95
    # 2011-03-04  socks     12.23
    # 2011-05-05  candles   0.99
    #
    # collect(DATE, ITEM, PRICE, [shoes, socks])
    # generates
    # DATE        ITEM_shoes ITEM_socks
    # 2010-01-01  23.45      5.95
    # 2011-03-04  nil        12.23
    #
    # The candles date is dropped
    def collect(by_field, per_field, value_field, per_values = nil, options = {})
      per_values.map!(&:to_sym) if per_values
      options = {:key_prefix => ''}.merge(options)
      by_value = nil
      template_row = {}
      if per_values # support placeholders
        per_values.each do |v|
          template_row[(options[:key_prefix] + v.to_s).to_sym] = nil
        end
      end
      outrow = template_row.dup
      self.reshape do |row, yielder|
        # p row
        if row
          if row[by_field] != by_value #start next row
            if by_value #emit previous if it exists
              yielder.yield(Dataframe::Row(outrow))
            end
            outrow = template_row.dup
            by_value = row[by_field]
            outrow[by_field] = by_value
          end
          outrow[(options[:key_prefix] + row[per_field].to_s).to_sym] = row[value_field] if per_values.nil? || per_values.include?(row[per_field].to_sym)
        else #get the last row
          yielder.yield(Dataframe::Row(outrow))
          by_value = nil
        end
      end
    end

    # assumes dataset is already sorted
    # inputs are formatted like
    # by = a field or array of fields
    # rules = a list of name => lambdafunc pairs
    # the name will be the names of fields in new collection
    # the lambda computes the value given a array of input values
    # no this does not scale to millions of rows AT ALL
    def group(by, *rules)
      by = [by].flatten
      rules = rules.shift
      collector_row = {}
      group_value = nil
      self.reshape do |row, yielder|
        if row
          if row.pick(*by) != group_value
            if group_value
              outrow = {}
              # binding.pry
              rules.each do |field, rule|
                outrow[field] = rule.call(collector_row)
              end
              yielder.yield(Dataframe::Row(outrow))
            end
            (row.keys + collector_row.keys).each do |k|
              collector_row[k] = [row[k]]
            end
            group_value = row.pick(*by)
          else
            collector_row.keys.each do |k|
              collector_row[k].push(row[k])
            end
          end
        else
          outrow = {}
          rules.each do |field, rule|
            outrow[field] = rule.call(collector_row)
          end
          yielder.yield(Dataframe::Row(outrow))
          group_value = nil
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
        Dataframe.Row(crow)
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
    # criticism: This is actually a join...
    # could be done as
    # new_data_set_with_all_desired_keys.join(:key, some rule about nulling)
    def fill(*mapping)
      mapping = mapping.first
      # exception if mapping length other than to
      throw Dataframe::ArgumentError.new('fill(:key => value_array) required') unless
        mapping.is_a?(Hash) && mapping.count == 1 && mapping.values.first.is_a?(Array)

      key = mapping.keys.first
      values = {}
      mapping.values.first.each do |value|
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
    # TODO - make some tests...
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

    # # arg: :column_name => column_rule
    # # nil out values that don't match given criteria
    # # can't build until I've figured out how to formulate the rules
    # # criticism: This is just compute...
    # # don't implement
    # def normalize_values(*rules)
    #   # TODO
    # end

    # TODO options hash?
    def merge(other_collection, merge_keys, options = {:merge_type => :ignore})
      # :ignore, :append, :replace, :fail
      # run through this, build index - append if desired

    end

    def sort(fieldname = nil, &block)
      if fieldname
        Dataframe::Table.new(self.all.sort {|a,b| a[fieldname] <=> b[fieldname]})
      else
        Dataframe::Table.new(self.all.sort(&block))
      end
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

    def values
      self.map(&:values)
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
