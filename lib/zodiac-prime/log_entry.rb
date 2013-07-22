module ZodiacPrime
  class LogEntry
    def initialize(term, command)
      @term = term
      @command = command
    end

    attr_reader :term, :command
  end
end
