module ZodiacPrime
  class Node
    def initialize(id, handler, timer, cluster)
      @node_id = id
      @handler = handler
      @timer = timer
      @cluster = cluster

      @current_term = 0
      @voted_for = nil
      @log = []
      @last_commit = nil

      become_follower
    end

    attr_reader :node_id
    attr_reader :current_term, :voted_for, :log, :role, :last_commit
    attr_reader :election_timeout

    def valid_vote?(opts)
      return false if opts[:term] < @current_term
      return false if @voted_for and opts[:candidate_id] != @voted_for

      true
    end

    def valid_log?(opts)
      if !@log.empty?
        return false if @log.last.term > opts[:last_log_term]
        return false if @log.size - 1 > opts[:last_log_index]
      end

      true
    end

    private :valid_vote?

    def request_vote(opts)
      unless valid_vote?(opts)
        return { :term => @current_term, :vote_granted => false }
      end

      if opts[:term] > @current_term
        @current_term = opts[:term]
        if @role != :follower
          become_follower(false)
        end
      end

      unless valid_log?(opts)
        return { :term => @current_term, :vote_granted => false }
      end

      @election_timeout = @timer.next
      @voted_for = opts[:candidate_id]
      @current_term = opts[:term]

      { :term => @current_term, :vote_granted => true }
    end

    def append_entries(opts)
      if @current_term > opts[:term]
        return { :success => false }
      end

      @current_term = opts[:term]

      become_follower

      unless @log.empty?
        if @log[opts[:prev_log_index]].term != opts[:prev_log_term]
          return { :success => false }
        end

        if @log.size - 1 > opts[:prev_log_index]
          @log[opts[:prev_log_index]+1..-1] = []
        end
      end

      if ent = opts[:entries]
        @log += ent
      end

      if opts[:commit_index]
        start = @last_commit ? @last_commit + 1 : 0
        start.upto(opts[:commit_index]).each do |idx|
          @handler << @log[idx].command
        end

        @last_commit = opts[:commit_index]
      end

      { :success => true }
    end

    def tick
      case @role
      when :follower, :candidate
        if @election_timeout and Time.now > @election_timeout
          become_candidate
        end
      end
    end

    def become_leader
      @role = :leader
      @cluster.reset_index @log.size

      opts = {
        :term => @current_term,
        :leader_id => @node_id,
        :prev_log_index => (@log.empty? ? nil : @log.size - 1),
        :prev_log_term  => (@log.empty? ? nil : @log.last.term),
        :entries => [],
        :commit_index => nil
      }

      @cluster.broadcast_entries opts
    end

    def become_candidate
      @role = :candidate
      @current_term += 1
      @election_timeout = @timer.next

      opts = {
        :term => @current_term,
        :candidate_id => @node_id,
        :last_log_index => (@log.empty? ? nil : @log.size - 1),
        :last_log_term  => (@log.empty? ? nil : @log.last.term)
      }

      @cluster.broadcast_vote_request opts
    end

    def become_follower(reset_timer=true)
      @role = :follower
      @election_timeout = @timer.next if reset_timer
    end

    def election_update(election)
      if election.highest_term > @current_term
        become_follower
      elsif election.won?
        @role = :leader
      end
    end

    def accept_command(cmd)
      prev_index = nil
      prev_term = nil

      unless @log.empty?
        prev_index = @log.size - 1
        prev_term = @log.last.term
      end

      log = LogEntry.new(@current_term, cmd)

      @log << log

      opts = {
        :term => @current_term,
        :leader_id => @node_id,
        :prev_log_index => prev_index,
        :prev_log_term  => prev_term,
        :entries => [log],
        :commit_index => nil
      }

      @cluster.broadcast_entries opts
    end

    def majority_accepted(idx)
      entry = @log.at(idx)
      @handler << entry.command

      opts = {
        :term => @current_term,
        :leader_id => @node_id,
        :prev_log_index => @log.size - 1,
        :prev_log_term  => @log.last.term,
        :entries => [],
        :commit_index => idx
      }

      @cluster.broadcast_entries opts
    end
  end
end
