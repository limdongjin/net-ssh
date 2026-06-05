require 'net/ssh/connection/event_loop'

module Net
  module SSH
    module Connection
      # Single-session event loop that cooperates with Ruby's Fiber scheduler.
      #
      # This keeps the normal Session#loop / Session#process contract intact, but
      # replaces the blocking IO.select wait with Fiber.scheduler.io_wait when a
      # scheduler is installed. It is opt-in; the default event loop is unchanged.
      class FiberSchedulerEventLoop < SingleSessionEventLoop
        private

        def io_select(readers, writers, timeout)
          scheduler = Fiber.respond_to?(:scheduler) ? Fiber.scheduler : nil
          return super unless scheduler

          wait_with_scheduler(readers, writers, timeout, scheduler)
        end

        def wait_with_scheduler(readers, writers, timeout, scheduler)
          readers = Array(readers).compact
          writers = Array(writers).compact
          return [[], [], []] if readers.empty? && writers.empty?

          deadline = timeout && Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

          loop do
            ready_readers, ready_writers = ready_ios(readers, writers)
            return [ready_readers, ready_writers, []] unless ready_readers.empty? && ready_writers.empty?

            remaining = remaining_timeout(deadline)
            return [nil, nil, nil] if remaining == 0

            wait_once(readers, writers, remaining, scheduler)
          end
        end

        def ready_ios(readers, writers)
          ready_readers = readers.select { |io| readable_now?(io) }
          ready_writers = writers.select { |io| writable_now?(io) }
          [ready_readers, ready_writers]
        end

        def wait_once(readers, writers, timeout, scheduler)
          wait_io = readers.first || writers.first
          events = 0
          events |= IO::READABLE if readers.include?(wait_io)
          events |= IO::WRITABLE if writers.include?(wait_io)
          scheduler.io_wait(wait_io, events, timeout)
        end

        def remaining_timeout(deadline)
          return nil unless deadline

          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          remaining.positive? ? remaining : 0
        end

        def readable_now?(io)
          IO.select([io], nil, nil, 0)&.first&.include?(io)
        end

        def writable_now?(io)
          IO.select(nil, [io], nil, 0)&.[](1)&.include?(io)
        end
      end
    end
  end
end
