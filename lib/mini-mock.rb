class MiniMockSink
  def initialize
    @called = []
  end

  def args
    raise "More than one call" if @called.size > 1
    @called.first
  end

  def called(args)
    @called << args
  end

  def times_called
    @called.size
  end

  def called?
    !@called.empty?
  end
end

class Object
  def mock(method)
    sink = MiniMockSink.new

    m = (class << self; self; end)

    m.send :define_method, method do |*args|
      sink.called args
    end

    sink
  end
end
