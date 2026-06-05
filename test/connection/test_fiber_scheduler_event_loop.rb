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

      class ZeroMaskScheduler
        attr_reader :calls

        def initialize
          @calls = []
        end

        def io_wait(io, events, timeout)
          @calls << [io, events, timeout]
          0
        end
      end

      class SleepingScheduler
        attr_reader :sleeps

        def initialize
          @sleeps = []
        end

        def kernel_sleep(timeout = nil)
          @sleeps << timeout
          nil
        end
      end

      class FailingScheduler
        def io_wait(*)
          raise "scheduler should not be used"
        end
      end

      class FakeSession
        attr_reader :handled_readers, :handled_writers, :postprocess_was_events

        def initialize(reader)
          @reader = reader
        end

        def ev_do_calculate_rw_wait(wait)
          [[@reader], [], wait]
        end

        def ev_do_handle_events(readers, writers)
          @handled_readers = readers
          @handled_writers = writers
        end

        def ev_do_postprocess(was_events)
          @postprocess_was_events = was_events
          true
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

      def test_zero_readiness_mask_returns_timeout_tuple
        reader, writer = IO.pipe
        scheduler = ZeroMaskScheduler.new
        loop = TestLoop.new
        loop.scheduler = scheduler

        assert_equal [nil, nil, nil], loop.send(:io_select, [reader], [], 1)
        assert_equal 1, scheduler.calls.length
      ensure
        reader&.close
        writer&.close
      end

      def test_event_loop_dispatch_uses_scheduler_selected_io
        reader, writer = IO.pipe
        scheduler = WakingScheduler.new(writer)
        loop = TestLoop.new
        loop.scheduler = scheduler
        session = FakeSession.new(reader)
        loop.register(session)

        loop.ev_select_and_postprocess(1)

        assert_equal [reader], session.handled_readers
        assert_equal [], session.handled_writers
        assert_equal true, session.postprocess_was_events
      ensure
        reader&.close
        writer&.close
      end

      def test_no_io_uses_scheduler_sleep
        scheduler = SleepingScheduler.new
        loop = TestLoop.new
        loop.scheduler = scheduler

        assert_equal [nil, nil, nil], loop.send(:io_select, [], [], 0.25)
        assert_equal [0.25], scheduler.sleeps
      end

      def test_no_io_zero_timeout_returns_timeout_tuple
        scheduler = SleepingScheduler.new
        loop = TestLoop.new
        loop.scheduler = scheduler

        assert_equal [nil, nil, nil], loop.send(:io_select, [], [], 0)
        assert_equal [], scheduler.sleeps
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
