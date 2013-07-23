require "test/unit"
require "zodiac-prime/node"
require "zodiac-prime/log_entry"

require "zodiac-prime/test"
require "zodiac-prime/election"

require 'mini-mock'

class TestZodiacPrimeNode < Test::Unit::TestCase
  def assert_called(m, count=1)
    assert_equal count, m.times_called
  end

  def new_node(id)
    ZodiacPrime::Node.new(id, @handler, @timer, @cluster)
  end

  attr_accessor :node

  def log_entry(term, command=nil)
    ZodiacPrime::LogEntry.new(term, command)
  end

  class StubTimer
    attr_writer :next

    def initialize
      @next = nil
    end

    def next
      v, @next = @next, nil
      v
    end
  end

  class StubCluster
    def initialize
      @leader_request = nil
    end

    attr_reader :leader_request

    def broadcast_vote_request(opts)
      @leader_request = opts
    end

    def reset_index(term)
    end

    def broadcast_entries(opts)
    end
  end

  def setup
    @cluster = StubCluster.new
    @timer = StubTimer.new
    @handler = []
    @node = new_node(0)
  end

  def test_state
    n = node
    assert_equal 0, n.node_id
    assert_equal 0, n.current_term
    assert_equal nil, n.voted_for
    assert_equal [], n.log
  end

  def test_request_vote_issues_vote
    res = node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 1, res[:term]
    assert_equal true, res[:vote_granted]

    assert_equal 1, node.voted_for
    assert_equal 1, node.current_term
  end

  def test_request_vote_from_stale
    node.current_term = 2
    res = node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 2, res[:term]
    assert_equal false, res[:vote_granted]

    assert_equal nil, node.voted_for
    assert_equal 2, node.current_term
  end

  def test_request_vote_from_same_term
    node.current_term = 1
    res = node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0
    assert_equal 1, res[:term]
    assert_equal true, res[:vote_granted]

    assert_equal 1, node.voted_for
    assert_equal 1, node.current_term
  end

  def test_request_vote_resets_role_to_follower_on_higher_term
    node.role = :candidate

    t = Time.now + 2
    @timer.next = t

    m = node.mock(:become_follower)

    node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_called m
  end

  def test_request_vote_doesnt_change_voted_for_with_outstanding_vote
    node.voted_for = 2
    node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_equal 2, node.voted_for
  end

  def test_request_vote_fails_if_local_log_last_entry_has_higher_term
    node = new_node(1)
    node.log = [log_entry(1)]

    res = node.request_vote :term => 2, :candidate_id => 1, :last_log_index => 1, :last_log_term => 0

    assert_equal false, res[:vote_granted]
  end

  def test_request_vote_fails_if_local_log_is_longer
    node.log = [log_entry(0), log_entry(0)]

    res = node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_equal false, res[:vote_granted]
  end

  def test_request_vote_resets_to_follower_even_if_log_check_fails
    node = new_node(1)
    node.role = :candidate

    node.log = [log_entry(1)]

    m = node.mock(:become_follower)

    res = node.request_vote :term => 2, :candidate_id => 1, :last_log_index => 1, :last_log_term => 0

    assert_equal false, res[:vote_granted]
    assert_called m
  end

  def test_request_vote_doesnt_resets_timer_even_if_log_check_fails
    t = Time.now + 2
    @timer.next = t

    node = new_node(1)
    node.role = :candidate
    assert_equal t, node.election_timeout

    node.log = [log_entry(1)]

    res = node.request_vote :term => 2, :candidate_id => 1, :last_log_index => 1, :last_log_term => 0

    assert_equal false, res[:vote_granted]
    assert_equal t, node.election_timeout
  end

  def test_request_vote_resets_election_timer
    t = Time.now + 0.200
    @timer.next = t

    node.request_vote :term => 1, :candidate_id => 1, :last_log_index => 0, :last_log_term => 0

    assert_equal t, node.election_timeout
  end

  ## append_entries

  def test_append_entries_ignores_outdated_term
    node.current_term = 1

    res = node.append_entries :term => 0

    assert_equal false, res[:success]
  end

  def test_append_entries_updates_current_term_when_greater
    node.append_entries :term => 1

    assert_equal 1, node.current_term
  end

  def test_append_entries_resets_state_to_follower
    t = Time.now + 2
    @timer.next = t

    m = node.mock(:become_follower)

    node.role = :leader
    node.append_entries :term => 1, :prev_log_index => 0,
                                    :prev_log_term => 0

    assert_called m
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

  def test_append_entries_resets_election_timer
    t = Time.now + 0.200
    @timer.next = t

    node.append_entries :term => 1, :prev_log_index => 0,
                                    :prev_log_term => 0

    assert_equal t, node.election_timeout
  end

  def test_become_candidate_broadcasts_votes
    node.log = [log_entry(0)]
    node.become_candidate

    req = @cluster.leader_request

    assert_equal node.current_term, req[:term]
    assert_equal node.node_id, req[:candidate_id]
    assert_equal 0, req[:last_log_index]
    assert_equal 0, req[:last_log_term]
  end

  def test_become_candidate_broadcasts_votes_when_empty
    node.become_candidate

    req = @cluster.leader_request

    assert_equal node.current_term, req[:term]
    assert_equal node.node_id, req[:candidate_id]
    assert_equal nil, req[:last_log_index]
    assert_equal nil, req[:last_log_term]
  end

  ## tick

  def test_tick_timeout_follower_to_candidate
    node.role = :follower
    node.election_timeout = Time.now - 1

    t = Time.now + 2
    @timer.next = t

    node.tick

    assert_equal :candidate, node.role
    assert_equal 1, node.current_term
    assert_equal t, node.election_timeout
  end

  def test_tick_broadcasts_vote_requests_on_becoming_candidate
    node.role = :follower
    node.election_timeout = Time.now - 1

    t = Time.now + 2
    @timer.next = t

    node.tick

    assert @cluster.leader_request, "no request broadcast"
  end

  def test_tick_timeout_on_canidate_restarts_election
    node.role = :candidate
    node.election_timeout = Time.now - 1

    node.tick

    assert_equal :candidate, node.role
    assert_equal 1, node.current_term
  end

  def test_tick_broadcasts_vote_requests_on_candidate_reelection
    node.role = :candidate
    node.election_timeout = Time.now - 1

    t = Time.now + 2
    @timer.next = t

    node.tick

    assert @cluster.leader_request, "no request broadcast"
  end

  def test_tick_does_nothing_on_leader
    node.role = :leader
    node.election_timeout = Time.now - 1

    node.tick

    assert_equal :leader, node.role
    assert_equal 0, node.current_term
  end

  ## election update

  def test_election_update_when_won_changes_to_leader
    e = ZodiacPrime::Election.new(3)
    e.receive_vote 1, :term => 0, :vote_granted => true
    e.receive_vote 2, :term => 0, :vote_granted => true
    node.role = :candidate

    node.election_update e

    assert_equal :leader, node.role
  end

  def test_election_update_when_higher_term_detected
    e = ZodiacPrime::Election.new(3)
    e.receive_vote 1, :term => 0, :vote_granted => true
    e.receive_vote 2, :term => 1, :vote_granted => true
    node.role = :candidate

    m = node.mock(:become_follower)

    node.election_update e

    assert_called m
  end

  ## become_follower

  def test_become_follower_sets_role
    node.role = :candidate
    node.become_follower

    assert_equal :follower, node.role
  end

  def test_become_follower_resets_election_timeout
    t = Time.now + 2
    @timer.next = t

    node.become_follower
    assert_equal t, node.election_timeout
  end

  def test_become_follower_doesnt_reset_timer_on_request
    t = Time.now

    node.election_timeout = t
    node.become_follower false

    assert_equal t, node.election_timeout
  end

  ## become_leader

  def test_become_leader_sets_role
    node.become_leader

    assert_equal :leader, node.role
  end

  def test_become_leader_sets_next_index
    m = @cluster.mock(:reset_index)

    node.log = [log_entry(0)]
    node.become_leader

    assert_called m
    assert_equal [1], m.args
  end

  def test_become_leader_sends_heartbeat
    node.current_term = 3
    node.log = [log_entry(0), log_entry(3)]

    m = @cluster.mock(:broadcast_entries)

    node.become_leader

    opts = {
      :term => node.current_term,
      :leader_id => node.node_id,
      :prev_log_index => 1,
      :prev_log_term => 3,
      :entries => [],
      :commit_index => nil
    }

    assert_equal [opts], m.args
  end

  def test_accept_command_append_to_log
    cur = node.log.size

    node.accept_command :foo

    assert_equal cur + 1, node.log.size
    assert_equal :foo, node.log.last.command
    assert_equal 0, node.log.last.term
  end

  def test_accept_command_transmits_command
    m = @cluster.mock(:broadcast_entries)

    node.accept_command :foo

    opts = {
      :term => node.current_term,
      :leader_id => node.node_id,
      :prev_log_index => nil,
      :prev_log_term => nil,
      :entries => [node.log.last],
      :commit_index => nil
    }

    assert_equal [opts], m.args
  end

  def test_accept_command_transmits_command_when_there_are_already_logs
    node.log = [log_entry(0), log_entry(0)]

    m = @cluster.mock(:broadcast_entries)

    node.accept_command :foo

    opts = {
      :term => node.current_term,
      :leader_id => node.node_id,
      :prev_log_index => 1,
      :prev_log_term => 0,
      :entries => [node.log.last],
      :commit_index => nil
    }

    assert_equal [opts], m.args
  end

  def test_majority_accepted_writes_commands_to_handler
    node.log = [log_entry(0, :foo)]

    node.majority_accepted 0

    assert_equal [:foo], @handler
  end
  
  def test_majority_accepted_writes_command_at_log_index
    node.log = [log_entry(0, :foo), log_entry(0, :blah)]

    node.majority_accepted 0

    assert_equal [:foo], @handler
  end
  
  def test_majority_accepted_broadcasts_commit
    node.log = [log_entry(0, :foo)]

    m = @cluster.mock(:broadcast_entries)

    node.majority_accepted 0

    opts = {
      :term => node.current_term,
      :leader_id => node.node_id,
      :prev_log_index => 0,
      :prev_log_term => 0,
      :entries => [],
      :commit_index => 0
    }

    assert_equal [opts], m.args
  end
  
end
