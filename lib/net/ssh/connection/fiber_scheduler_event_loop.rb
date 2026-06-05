require 'net/ssh/connection/event_loop'

module Net
  module SSH
    module Connection
      class FiberSchedulerEventLoop < SingleSessionEventLoop
        def ev_select_and_postprocess(wait)
          raise "Only one session expected" unless @sessions.count == 1

          session = @sessions.first
          readers, writers, timeout = session.ev_do_calculate_rw_wait(wait)
          ready_readers, ready_writers, = io_select(readers, writers, timeout)

          session.ev_do_handle_events(ready_readers, ready_writers)
          session.ev_do_postprocess(!((ready_readers.nil? || ready_readers.empty?) &&
                                      (ready_writers.nil? || ready_writers.empty?)))
        end

        private

        def io_select(readers, writers, timeout)
          readers = Array(readers).compact
          writers = Array(writers).compact
          scheduler = current_scheduler

          return wait_without_io(timeout, scheduler) if readers.empty? && writers.empty?
          return blocking_io_select(readers, writers, timeout) unless scheduler
          return blocking_io_select(readers, writers, timeout) unless scheduler.respond_to?(:io_wait)
          return blocking_io_select(readers, writers, timeout) if timeout == 0

          ios = (readers + writers).uniq
          return blocking_io_select(readers, writers, timeout) unless ios.length == 1

          wait_with_scheduler(ios.first, readers, writers, timeout, scheduler)
        rescue NotImplementedError
          blocking_io_select(readers, writers, timeout)
        end

        def current_scheduler
          Fiber.respond_to?(:scheduler) ? Fiber.scheduler : nil
        end

        def wait_without_io(timeout, scheduler)
          return [nil, nil, nil] if timeout == 0

          if scheduler&.respond_to?(:kernel_sleep)
            scheduler.kernel_sleep(timeout)
            [nil, nil, nil]
          else
            blocking_io_select([], [], timeout)
          end
        end

        def blocking_io_select(readers, writers, timeout)
          IO.select(readers, writers, nil, timeout) || [nil, nil, nil]
        end

        def wait_with_scheduler(io, readers, writers, timeout, scheduler)
          deadline = timeout && Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
          events = wait_events(io, readers, writers)

          loop do
            ready_readers, ready_writers = ready_ios(io, readers, writers)
            return [ready_readers, ready_writers, []] unless ready_readers.empty? && ready_writers.empty?

            remaining = remaining_timeout(deadline)
            return [nil, nil, nil] if remaining == 0

            ready = scheduler.io_wait(io, events, remaining)
            selected = selected_from_mask(io, readers, writers, ready)
            return selected if selected
          end
        end

        def wait_events(io, readers, writers)
          events = 0
          events |= IO::READABLE if readers.include?(io)
          events |= IO::WRITABLE if writers.include?(io)
          events
        end

        def selected_from_mask(io, readers, writers, mask)
          return [nil, nil, nil] if mask.nil? || mask == false || mask == 0

          ready_readers = readers.include?(io) && (mask & IO::READABLE) != 0 ? [io] : []
          ready_writers = writers.include?(io) && (mask & IO::WRITABLE) != 0 ? [io] : []
          return nil if ready_readers.empty? && ready_writers.empty?

          [ready_readers, ready_writers, []]
        end

        def ready_ios(io, readers, writers)
          ready_readers = readers.include?(io) && readable_now?(io) ? [io] : []
          ready_writers = writers.include?(io) && writable_now?(io) ? [io] : []
          [ready_readers, ready_writers]
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
