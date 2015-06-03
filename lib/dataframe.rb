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
    def each(&block)
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

    def select(&block)
      new_collection = Enumerator.new do |yielder|
        self.each {|row| yielder.yield row if block.call(row)}
      end
      return Dataframe::Table.new(new_collection, Dataframe::Table.noop)
    end

    def columns(*names)
      new_chain = Proc.new do |row|
        chain.call(row).select {|k,v| names.map{|name| name.to_s}.include?(k.to_s)}
      end
      Dataframe::Table.new(self.raw_data, new_chain)
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
