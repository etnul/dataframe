require 'minitest_helper'
require 'pry'

class TestDataframe < Minitest::Test

  def setup
    @source1 = [
      {:foo => 1, :year => 2012, :bar => 'abe'},
      {:foo => 5, :year => 2013, :bar => 'banan'},
      {:foo => 45, :year => 2014, :bar => 'konto'}
    ]
    @source2 = [
      {:a => 1, :year => 2012, :key => 'abe'},
      {:a => 5, :year => 2012, :key => 'banan'},
      {:a => 45, :year => 2014, :key => 'kontoer'}
    ]
    @a = Dataframe::Table.new(@source1)
    @b = Dataframe::Table.new(@source2)
  end

  def test_that_it_has_a_version_number
    refute_nil ::Dataframe::VERSION
  end

  def test_compute_reshape

    bb = @b.compute(:tigangea) {|r| r.a*10}

    year = nil
    aasum = 0

    bbb = bb.reshape do |row, emitter|
      if row
        # p row
        if row.year != year
          if year
            emitter.yield Dataframe::Row({:aasum => aasum, :year => year})
          end
          aasum = row.tigangea
          year = row.year
        else
          aasum += row.tigangea
        end
      else
        if year
          emitter.yield Dataframe::Row({:aasum => aasum, :year => year})
        end
      end
    end

    result = bbb.all
    assert_equal result.count, 2
    assert_equal result[0].year, 2012
    assert_equal result[0].aasum, 10*(1+5)
    assert_equal result[1].year, 2014
    assert_equal result[1].year, 2014

  end

  def test_chainable
    @a.compute(:new_field) {|row| row.a * 10}.select {|row| a.new_field == 10}
  end

  def test_joins
    result = @b.join(@a, :year).all
    assert_equal 3, result.count
    rows2012 = result.select {|r| r.year == 2012}
    assert_equal 2, rows2012.count
    assert_equal 'abe', rows2012.first.bar
    assert_equal 2, rows2012.map {|r| r.foo}.inject(&:+)
  end

  # def test_radicals
  #   # r = Dataframe::Table.compute(:column_name) {}
  #   # Dataframe::Table.combine(r) # same as Dataframe::Table.compute(:column:name) {}
  #
  # end

end
