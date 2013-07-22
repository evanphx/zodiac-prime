module ZodiacPrime
  class Election
    def initialize(total)
      @total = total
      @majority = (total / 2) + 1
      @votes = {}
    end

    def votes
      @votes.size
    end

    def granted_votes
      granted = 0

      @votes.each { |n,o| granted += (o[:vote_granted] ? 1 : 0) }

      granted
    end

    def over?
      @votes.size >= @majority
    end

    def won?
      granted_votes >= @majority
    end

    def highest_term
      return nil if @votes.empty?

      highest = 0

      @votes.each { |n,o| highest = o[:term] if o[:term] > highest }

      highest
    end

    def receive_vote(node, opts)
      @votes[node] ||= opts
    end
  end
end
