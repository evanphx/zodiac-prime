module ZodiacPrime
  class Cluster
    def initialize(id, transmitter, handler, timer, peers)
      @transmitter = transmitter
      @handler = handler
      @timer = timer
      @peers = peers

      @this_node = Node.new id, handler, timer, self
    end

    attr_reader :this_node
  end
end
