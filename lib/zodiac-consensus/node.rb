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
    attr_accessor :current_term, :voted_for, :log, :role

    def request_vote(opts)
      if opts[:term] < @current_term
        return { :term => @current_term, :vote_granted => false }
      end

      if @voted_for and opts[:candidate_id] != @voted_for
        return { :term => @current_term, :vote_granted => false }
      end

      if !@log.empty?
        if @log.last.term > opts[:last_log_term]
          return { :term => @current_term, :vote_granted => false }
        end

        if @log.size - 1 > opts[:last_log_index]
          return { :term => @current_term, :vote_granted => false }
        end
      end

      @voted_for = opts[:candidate_id]
      @current_term = opts[:term]
      { :term => @current_term, :vote_granted => true }
    end
  end
end
