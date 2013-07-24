require "test/unit"
require "zodiac-prime/node"
require "zodiac-prime/log_entry"

require "zodiac-prime/test"
require "zodiac-prime/cluster"

require 'mini-mock'

class TestZodiacPrimeCluster < Test::Unit::TestCase
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

  def setup
    @peer = 1
    @peers = [@peer]
    @handler = []
    @transmitter = Object.new
    @timer = StubTimer.new
    @cluster = ZodiacPrime::Cluster.new 0, @transmitter, @handler, @timer, @peers
    @node = @cluster.this_node
  end

  attr_reader :node, :peer, :transmitter, :cluster

  def test_reset_index
    cluster.next_indices[@peer] = 1
    cluster.reset_index

    assert_equal 0, cluster.next_indices[@peer]
  end

  def test_replicate_calls_transmit
    node.log = [ZodiacPrime::LogEntry.new(0, :foo)]

    m = transmitter.mock(:send_message)

    opts = {
      :term => @node.current_term,
      :leader_id => @node.node_id,
      :prev_log_index => nil,
      :prev_log_term => nil,
      :entries => [node.log.last],
      :commit_index => nil
    }

    cluster.replicate

    assert_equal [1, :append_entries, opts], m.args[0,3]
  end

  def test_replicate_callback_advances_nextindex_on_success
    node.log = [ZodiacPrime::LogEntry.new(0, :foo)]

    m = transmitter.mock(:send_message)

    opts = {
      :term => @node.current_term,
      :leader_id => @node.node_id,
      :prev_log_index => nil,
      :prev_log_term => nil,
      :entries => [node.log.last],
      :commit_index => nil
    }

    cluster.replicate

    blk = m.args.last

    blk.call :term => 0, :success => true

    assert_equal 1, cluster.next_indices[0]
  end

  def test_replicate_does_nothing_if_peers_up_to_date
    cluster.next_indices[peer] = 1
    node.log = [ZodiacPrime::LogEntry.new(0, :foo)]

    m = transmitter.mock(:send_message)

    assert_equal 0, m.times_called
  end

  def test_replicate_includes_data_about_previous_log_entry
    cluster.next_indices[peer] = 1

    node.log = [ZodiacPrime::LogEntry.new(0, :foo),
                ZodiacPrime::LogEntry.new(0, :bar)]

    m = transmitter.mock(:send_message)

    opts = {
      :term => @node.current_term,
      :leader_id => @node.node_id,
      :prev_log_index => 0,
      :prev_log_term => 0,
      :entries => [node.log.last],
      :commit_index => nil
    }

    assert_equal [1, :append_entries, opts], m.args[0,3]
  end

  def test_replicate_sends_to_all_peers
    @peers << 2

    cluster.next_indices[peer] = 1

    node.log = [ZodiacPrime::LogEntry.new(0, :foo),
                ZodiacPrime::LogEntry.new(0, :bar)]

    m = transmitter.mock(:send_message)

    opts = {
      :term => @node.current_term,
      :leader_id => @node.node_id,
      :prev_log_index => 0,
      :prev_log_term => 0,
      :entries => [node.log.last],
      :commit_index => nil
    }

    assert_equal [1, :append_entries, opts], m.args[0,3]

    opts = {
      :term => @node.current_term,
      :leader_id => @node.node_id,
      :prev_log_index => nil,
      :prev_log_term => nil,
      :entries => [node.log.last],
      :commit_index => nil
    }

    assert_equal [2, :append_entries, opts], m.args[0,3]
  end
end
