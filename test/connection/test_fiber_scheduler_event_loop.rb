require 'common'
require 'net/ssh/connection/fiber_scheduler_event_loop'

module NetSSH
  module Connection
    class FiberSchedulerEventLoopTest < NetSSHTest
      class TestLoop < Net::SSH::Connection::FiberSchedulerEventLoop
        attr_accessor :scheduler

        private

        def current_scheduler
          scheduler
        end
      end

      class WakingScheduler
        attr_reader :calls

        def initialize(writer)
          @writer = writer
          @calls = []
        end

        def io_wait(io, events, timeout)
          @calls << [io, events, timeout]
          @writer.write("x")
          IO::READABLE
        end
      end

      class FailingScheduler
        def io_wait(*)
          raise "scheduler should not be used"
        end
      end

      def test_single_io_uses_scheduler_wait
        reader, writer = IO.pipe
        scheduler = WakingScheduler.new(writer)
        loop = TestLoop.new
        loop.scheduler = scheduler

        readers, writers, = loop.send(:io_select, [reader], [], 1)

        assert_equal [reader], readers
        assert_equal [], writers
        assert_equal 1, scheduler.calls.length
        assert_equal reader, scheduler.calls.first[0]
        assert_equal IO::READABLE, scheduler.calls.first[1]
      ensure
        reader&.close
        writer&.close
      end

      def test_multiple_ios_fall_back_to_io_select
        reader1, writer1 = IO.pipe
        reader2, writer2 = IO.pipe
        writer2.write("x")

        loop = TestLoop.new
        loop.scheduler = FailingScheduler.new

        readers, = loop.send(:io_select, [reader1, reader2], [], 1)

        assert_equal [reader2], readers
      ensure
        [reader1, writer1, reader2, writer2].each { |io| io&.close unless io.closed? }
      end

      def test_zero_timeout_falls_back_to_polling_select
        reader, writer = IO.pipe
        writer.write("x")
        loop = TestLoop.new
        loop.scheduler = FailingScheduler.new

        readers, = loop.send(:io_select, [reader], [], 0)

        assert_equal [reader], readers
      ensure
        reader&.close
        writer&.close
      end
    end
  end
end
