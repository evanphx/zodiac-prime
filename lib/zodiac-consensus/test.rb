require 'zodiac-consensus/node'

module ZodiacConsensus
  class Node
    attr_writer :current_term, :voted_for, :log, :role
  end
end
