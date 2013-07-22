module ZodiacPrime
  class Node
    def initialize(id, handler)
      @node_id = id
      @handler = handler
      @current_term = 0
      @voted_for = nil
      @log = []
      @role = :follower
      @last_commit = nil
    end

    attr_reader :node_id
    attr_reader :current_term, :voted_for, :log, :role, :last_commit

    def valid_vote?(opts)
      return false if opts[:term] < @current_term
      return false if @voted_for and opts[:candidate_id] != @voted_for

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

      @voted_for = opts[:candidate_id]
      @current_term = opts[:term]
      { :term => @current_term, :vote_granted => true }
    end

    def append_entries(opts)
      if @current_term > opts[:term]
        return { :success => false }
      end

      @current_term = opts[:term]
      @role = :follower

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
  end
end