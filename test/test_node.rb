require "test/unit"
require "zodiac-consensus/node"
require "zodiac-consensus/log_entry"

class TestZodiacConsensusNode < Test::Unit::TestCase
  def new_node(id)
    ZodiacConsensus::Node.new(id)
  end

  attr_accessor :node

  def log_entry(term)
    ZodiacConsensus::LogEntry.new(term, nil)
  end

  def setup
    @node = new_node(0)
  end

  def test_state
    n = @node
    assert_equal 0, n.node_id
    assert_equal 0, n.current_term
    assert_equal nil, n.voted_for
    assert_equal [], n.log
  end

  def test_request_vote_issues_vote
    res = @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 1, res[:term]
    assert_equal true, res[:vote_granted]

    assert_equal 1, node.voted_for
    assert_equal 1, node.current_term
  end

  def test_request_vote_from_stale
    @node.current_term = 2
    res = @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 2, res[:term]
    assert_equal false, res[:vote_granted]

    assert_equal nil, node.voted_for
    assert_equal 2, node.current_term
  end

  def test_request_vote_from_same_term
    @node.current_term = 1
    res = @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 1, res[:term]
    assert_equal true, res[:vote_granted]

    assert_equal 1, node.voted_for
    assert_equal 1, node.current_term
  end

  def test_request_vote_resets_state_to_follower_on_higher_term
    @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
  
    assert_equal :follower, @node.role
  end

  def test_request_vote_doesnt_change_voted_for_with_outstanding_vote
    @node.voted_for = 2
    @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_equal 2, @node.voted_for
  end

  def test_request_vote_fails_if_local_log_last_entry_has_higher_term
    @node = new_node(1)
    @node.log = [log_entry(1)]

    res = @node.request_vote :term => 2, :candidate_id => 1, :last_log_index => 1, :last_log_term => 0

    assert_equal false, res[:vote_granted]
  end

  def test_request_vote_fails_if_local_log_is_longer
    @node.log = [log_entry(0), log_entry(0)]

    res = @node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_equal false, res[:vote_granted]
  end
end
