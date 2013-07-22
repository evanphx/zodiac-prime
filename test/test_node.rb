require "test/unit"
require "zodiac-prime/node"
require "zodiac-prime/log_entry"

require "zodiac-prime/test"

class TestZodiacPrimeNode < Test::Unit::TestCase
  def new_node(id)
    ZodiacPrime::Node.new(id, @handler)
  end

  attr_accessor :node

  def log_entry(term, command=nil)
    ZodiacPrime::LogEntry.new(term, command)
  end

  def setup
    @handler = []
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

  def test_request_vote_resets_role_to_follower_on_higher_term
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

  ## append_entries

  def test_append_entries_ignores_outdated_term
    @node.current_term = 1

    res = @node.append_entries :term => 0

    assert_equal false, res[:success]
  end

  def test_append_entries_updates_current_term_when_greater
    @node.append_entries :term => 1

    assert_equal 1, @node.current_term
  end

  def test_append_entries_resets_state_to_follower
    node.role = :leader
    node.append_entries :term => 1, :prev_log_index => 0,
                                    :prev_log_term => 0

    assert_equal :follower, node.role
  end

  def test_append_entries_fails_if_prev_term_check_fails
    node.log = [log_entry(1)]

    res = node.append_entries :term => 1, :prev_log_index => 0,
                              :prev_log_term => 0

    assert_equal false, res[:success]
  end

  def test_append_entries_discards_conflicting_entries_before_appending
    node.log = [log_entry(0), log_entry(0)]

    entries = [log_entry(1, :foo)]

    node.append_entries :term => 1, :prev_log_index => 0,
                                          :prev_log_term => 0,
                                          :entries => entries

    assert_equal 2, node.log.size
    assert_equal 1, node.log.last.term
    assert_equal :foo, node.log.last.command
  end

  def test_append_entries_commits_log_entries
    node.log = [log_entry(0, :a), log_entry(0, :b)]

    node.append_entries :term => 1, :prev_log_index => 1,
                                          :prev_log_term => 0,
                                          :entries => [],
                                          :commit_index => 1

    assert_equal [:a, :b], @handler
  end

  def test_append_entries_doesnt_recommit_already_commited_entries
    node.log = [log_entry(0, :a), log_entry(0, :b)]
    node.last_commit = 0

    node.append_entries :term => 1, :prev_log_index => 1,
                                          :prev_log_term => 0,
                                          :entries => [],
                                          :commit_index => 1

    assert_equal [:b], @handler
  end
end
