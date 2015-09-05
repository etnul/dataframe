require 'minitest_helper'
require 'pry'

class TestOperations < Minitest::Test

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
    @string_source = [
      {'a' => 1, :year => 2012, :key => 'abe'},
      {'a' => 5, :year => 2012, :key => 'banan'},
      {'a' => 45, :year => 2014, :key => 'kontoer'}
    ]
    @a = Dataframe::Table.new(@source1)
    @b = Dataframe::Table.new(@source2)
    @c = Dataframe::Table.new(@string_source)
  end

  def test_compute
    computed = @a.compute(:century) do |row|
      row.year/100
    end
    assert computed
    assert_equal computed.first.keys.count, 4
    assert_equal computed.all.map(&:century).uniq, [20]
  end

  def test_merge

  end

  def test_select
    selected = @a.select {|row| row.year == 2012}
    assert_equal selected.all.count, 1
    assert_equal selected.first, @a.first
  end

  def test_pick
    picked = @a.pick(:foo, :year, :new)
    assert picked
    assert_equal picked.all.first.keys.count, 3
    assert_equal picked.all.map(&:keys).flatten.uniq, [:foo, :year, :new]
    assert_equal picked.all.map(&:new).uniq, [nil]
  end

  def test_rename
    renamed = @a.rename(:bar => :baz, :noop => :frotz)
    assert renamed
    assert_equal renamed.all.first[:baz], @a.all.first[:bar]
  end

  def test_fill
    filled = @a.fill(:year => [1900, 1910, 1920, 1930])
    assert_equal filled.count, 7
  end

  def test_sort
    bigger_a = @a.fill(:year => (1..20).map {1900 + rand(100)})
    sorted = bigger_a.sort {|a,b| a[:year] <=> b[:year] }
    assert sorted.is_a?(Dataframe::Table)
    assert_equal sorted.map {|s| s[:year]}, sorted.map {|s| s[:year]}.sort
    field_sorted = bigger_a.sort(:year)
    assert_equal field_sorted.map {|s| s[:year]}, sorted.map {|s| s[:year]}
  end

  def test_from
  end

  def test_default
    defaulted = @a.pick(:year, :new).default(:new => 3)
    assert_equal defaulted.all.map {|r| r[:new]}.inject(&:+), 9
  end

  def test_normalize_values
  end

end
