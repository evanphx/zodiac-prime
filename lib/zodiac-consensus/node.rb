module ZodiacConsensus
  class Node
    def initialize(id)
      @node_id = id
      @current_term = 0
      @voted_for = nil
      @log = []
      @role = :follower
    end

    attr_reader :node_id
    attr_reader :current_term, :voted_for, :log, :role

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
  end
end
