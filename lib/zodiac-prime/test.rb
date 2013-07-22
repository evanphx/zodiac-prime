require 'zodiac-prime/node'

module ZodiacPrime
  class Node
    attr_writer :current_term, :voted_for, :log, :role, :last_commit
    attr_writer :election_timeout
  end
end
