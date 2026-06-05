require 'net/ssh/connection/event_loop'

module Net
  module SSH
    module Connection
      class FiberSchedulerEventLoop < SingleSessionEventLoop
        private

        def io_select(readers, writers, timeout)
          readers = Array(readers).compact
          writers = Array(writers).compact
          scheduler = Fiber.respond_to?(:scheduler) ? Fiber.scheduler : nil

          return super unless scheduler
          return super unless scheduler.respond_to?(:io_wait)
          return super if timeout == 0
          return super unless (readers + writers).uniq.length == 1

          wait_with_scheduler((readers + writers).uniq.first, readers, writers, timeout, scheduler)
        rescue NotImplementedError
          super
        end

        def wait_with_scheduler(io, readers, writers, timeout, scheduler)
          deadline = timeout && Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

          loop do
            ready_readers, ready_writers = ready_ios(io, readers, writers)
            return [ready_readers, ready_writers, []] unless ready_readers.empty? && ready_writers.empty?

            remaining = remaining_timeout(deadline)
            return [nil, nil, nil] if remaining == 0

            wait_once(io, readers, writers, remaining, scheduler)
          end
        end

        def ready_ios(io, readers, writers)
          ready_readers = readers.include?(io) && readable_now?(io) ? [io] : []
          ready_writers = writers.include?(io) && writable_now?(io) ? [io] : []
          [ready_readers, ready_writers]
        end

        def wait_once(io, readers, writers, timeout, scheduler)
          events = 0
          events |= IO::READABLE if readers.include?(io)
          events |= IO::WRITABLE if writers.include?(io)
          scheduler.io_wait(io, events, timeout)
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
