require 'minitest_helper'

class TestDataframeRow < Minitest::Test

  def test_wrapping
    assert Dataframe::Row({:a => 1}).is_a?(Dataframe::RowType)
    assert_equal Dataframe::Row({:a => 1}).a, 1
  end

  def test_access
    hash = {:name => 'value'}
    row = Dataframe::Row(hash)
    assert_equal row.name, 'value'
    row.additional_name = 'Another value'
    assert_equal hash[:additional_name], 'Another value'
  end

  def test_pick
    row = Dataframe::Row({:a => 1, :b => 2, :c => 3})
    picked = row.pick(:a, :b)
    assert_equal picked, [1,2]
  end

end
