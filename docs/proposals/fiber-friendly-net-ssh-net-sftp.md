# Proposal: Fiber-friendly `net-ssh` / `net-sftp` via Single-IO Scheduler Bridge

## Status

Draft.

## Summary

This proposal introduces an opt-in Fiber-friendly event loop for `net-ssh` that maps the common **single SSH session / single transport socket** readiness wait to Ruby's `Fiber.scheduler.io_wait`.

This proposal does **not** claim that `net-ssh` cannot cooperate with Ruby's scheduler today. Ruby's `IO.select` can already call `Fiber.scheduler.io_select` when executed inside a non-blocking Fiber and when the active scheduler implements that hook.

The proposed value is narrower: for the most common `net-ssh` / `net-sftp` workload, where one SSH session owns one transport socket, provide a direct `io_wait` path with a simpler readiness contract:

```text
one watched IO
read/write event mask
timeout
ready event mask
```

`net-sftp` can benefit without a major rewrite because its synchronous-looking APIs already drive the underlying SSH session loop. Optional `Request#await` / `Session#await_open` APIs may be added later, but the core scheduling improvement belongs in `net-ssh`.

## Motivation

`net-ssh` and `net-sftp` are commonly used through blocking-style APIs:

```ruby
Net::SSH.start(host, user) do |ssh|
  puts ssh.exec!("echo ok")
end
```

```ruby
Net::SFTP.start(host, user) do |sftp|
  sftp.upload!("/local/file", "/remote/file")
end
```

These APIs should remain supported.

Today, `net-ssh` uses `IO.select` at the event-loop readiness boundary. On Ruby versions and schedulers that implement `Fiber.scheduler.io_select`, this path may already cooperate with the scheduler. However, behavior and efficiency depend on the scheduler's `io_select` implementation.

For the common single SSH transport socket case, `net-ssh` can expose an opt-in event loop that uses:

```ruby
Fiber.scheduler.io_wait(io, events, timeout)
```

This gives schedulers a narrower readiness contract than broad multi-descriptor `IO.select`, while preserving the existing `IO.select` path for complex cases.

## Scope

### In scope

```text
- Single Net::SSH::Connection::Session
- Single transport socket
- Command execution through one SSH connection
- SFTP upload/download over one SSH connection
- Opt-in event loop injection
- Conservative fallback to existing IO.select path
```

### Out of scope for the first implementation

```text
- Multi-session shared event loop
- Port forwarding
- Listener sockets
- Multiple sockets in one event loop
- Proxy/jump-host connect path
- SSH agent socket integration
- DNS and TCP connect scheduler integration
- Full local file IO scheduling for large SFTP transfers
- Complete replacement of IO.select for arbitrary read/write/exception sets
```

## Background

### Ruby scheduler model

Ruby's Fiber scheduler provides hooks for operations that may block.

Relevant hooks include:

```ruby
io_wait(io, events, timeout)
io_select(readables, writables, exceptables, timeout)
kernel_sleep(duration = nil)
```

`IO.select` can be scheduler-aware through `io_select`. `IO#wait`, `IO#wait_readable`, and `IO#wait_writable` use `io_wait`.

Therefore, this proposal is not about making scheduler cooperation possible for the first time. It is about providing a **direct and predictable single-IO path** for the dominant SSH/SFTP case.

### net-ssh event loop injection

`net-ssh` already allows an event loop object to be injected through options:

```ruby
@event_loop = options[:event_loop] || SingleSessionEventLoop.new
@event_loop.register(self)
```

This means a Fiber-friendly event loop can be introduced without changing the main `Net::SSH.start` public API.

Usage:

```ruby
Net::SSH.start(
  host,
  user,
  event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
) do |ssh|
  puts ssh.exec!("echo ok")
end
```

A new event loop instance must be created per SSH session. The proposed event loop is single-session by design.

### net-sftp dependency on net-ssh

`net-sftp` builds on `net-ssh`. `Net::SFTP.start` creates an SSH session first, then creates and connects an SFTP session:

```ruby
session = Net::SSH.start(host, user, ssh_options)
sftp = Net::SFTP::Session.new(session, version, &block).connect!
```

SFTP waits ultimately drive the SSH session event loop. Therefore, `net-sftp` can benefit from a Fiber-friendly `net-ssh` event loop without requiring a full SFTP rewrite.

## Design

Add:

```ruby
Net::SSH::Connection::FiberSchedulerEventLoop
```

It should subclass `SingleSessionEventLoop`, but it must override the actual dispatch method, not only a helper.

The important method is:

```ruby
ev_select_and_postprocess(wait)
```

A helper-only approach is insufficient if `SingleSessionEventLoop#ev_select_and_postprocess` directly calls `IO.select`.

### Core event loop structure

```ruby
module Net
  module SSH
    module Connection
      class FiberSchedulerEventLoop < SingleSessionEventLoop
        def register(session)
          raise ArgumentError, "FiberSchedulerEventLoop supports only one session" unless @sessions.empty?

          super
        end

        def ev_select_and_postprocess(wait)
          raise "Only one session expected" unless @sessions.count == 1

          session = @sessions.first
          readers, writers, timeout = session.ev_do_calculate_rw_wait(wait)
          ready_readers, ready_writers, = io_select(readers, writers, timeout)

          session.ev_do_handle_events(ready_readers, ready_writers)
          session.ev_do_postprocess(
            !(Array(ready_readers).empty? && Array(ready_writers).empty?)
          )
        end
      end
    end
  end
end
```

`register` fails early if an event loop instance is reused across sessions.

## Scheduler path selection

Use the `io_wait` bridge only when:

```text
- a current non-blocking Fiber scheduler exists
- the scheduler responds to io_wait
- timeout is not 0
- exactly one unique watched object exists across readers and writers
```

Fallback to the existing `IO.select` path when:

```text
- no eligible scheduler exists
- scheduler does not respond to io_wait
- timeout == 0
- more than one watched object exists
- io_wait raises NotImplementedError
```

The fallback should be described as the **existing IO.select path**, not strictly as a blocking path, because `IO.select` itself may call `scheduler.io_select`.

## Reference implementation sketch

```ruby
require "net/ssh/connection/event_loop"

module Net
  module SSH
    module Connection
      class FiberSchedulerEventLoop < SingleSessionEventLoop
        def register(session)
          raise ArgumentError, "FiberSchedulerEventLoop supports only one session" unless @sessions.empty?

          super
        end

        def ev_select_and_postprocess(wait)
          raise "Only one session expected" unless @sessions.count == 1

          session = @sessions.first
          readers, writers, timeout = session.ev_do_calculate_rw_wait(wait)
          ready_readers, ready_writers, = io_select(readers, writers, timeout)

          session.ev_do_handle_events(ready_readers, ready_writers)
          session.ev_do_postprocess(
            !(Array(ready_readers).empty? && Array(ready_writers).empty?)
          )
        end

        private

        def io_select(readers, writers, timeout)
          readers = Array(readers).compact
          writers = Array(writers).compact
          scheduler = current_scheduler

          return wait_without_io(timeout, scheduler) if readers.empty? && writers.empty?
          return existing_io_select(readers, writers, timeout) unless scheduler&.respond_to?(:io_wait)
          return existing_io_select(readers, writers, timeout) if timeout == 0

          watched = (readers + writers).uniq
          return existing_io_select(readers, writers, timeout) unless watched.size == 1

          watched_object = watched.first
          wait_io = wait_target(watched_object)
          events = wait_events(watched_object, readers, writers)

          mask = scheduler.io_wait(wait_io, events, timeout)
          selected_from_mask(watched_object, readers, writers, mask) || [nil, nil, nil]
        rescue NotImplementedError
          existing_io_select(readers, writers, timeout)
        end

        def current_scheduler
          if Fiber.respond_to?(:current_scheduler)
            Fiber.current_scheduler
          elsif Fiber.respond_to?(:scheduler)
            return nil if Fiber.current.respond_to?(:blocking?) && Fiber.current.blocking?

            Fiber.scheduler
          end
        end

        def wait_target(object)
          object.respond_to?(:to_io) ? object.to_io : object
        end

        def wait_without_io(timeout, scheduler)
          return [nil, nil, nil] if timeout == 0

          if scheduler&.respond_to?(:kernel_sleep)
            scheduler.kernel_sleep(timeout)
            [nil, nil, nil]
          else
            existing_io_select([], [], timeout)
          end
        end

        def existing_io_select(readers, writers, timeout)
          IO.select(readers, writers, nil, timeout) || [nil, nil, nil]
        end

        def wait_events(watched_object, readers, writers)
          events = 0
          events |= IO::READABLE if readers.include?(watched_object)
          events |= IO::WRITABLE if writers.include?(watched_object)
          events
        end

        def selected_from_mask(watched_object, readers, writers, mask)
          return [nil, nil, nil] if mask.nil? || mask == false || mask == 0

          ready_readers =
            readers.include?(watched_object) && (mask & IO::READABLE) != 0 ? [watched_object] : []

          ready_writers =
            writers.include?(watched_object) && (mask & IO::WRITABLE) != 0 ? [watched_object] : []

          return nil if ready_readers.empty? && ready_writers.empty?

          [ready_readers, ready_writers, []]
        end
      end
    end
  end
end
```

This sketch intentionally avoids a preflight `IO.select(..., 0)` inside the `io_wait` bridge. The single-IO path is:

```text
single watched object
-> scheduler.io_wait(wait_io, events, timeout)
-> mask to IO.select-style result
```

## Alternative: `IO#wait`

The first implementation may call `scheduler.io_wait` directly for explicit mask mapping.

An alternative is to use `IO#wait(events, timeout)` if maintainers prefer avoiding direct calls to scheduler hooks. Ruby documents `io_wait` as the hook used by `IO#wait`, `IO#wait_readable`, and `IO#wait_writable`.

This alternative should be evaluated during upstream review.

Tradeoff:

```text
Direct scheduler.io_wait:
  - explicit mapping
  - easier to preserve original watched object in return arrays
  - more direct but calls scheduler hook manually

IO#wait:
  - less direct scheduler coupling
  - delegates hook/fallback behavior to Ruby
  - may require more care around wrapper objects and return semantics
```

## Important semantics

### `IO.select` path is preserved

Fallback deliberately uses `IO.select`.

This matters because:

```text
IO.select may itself cooperate with Fiber.scheduler through io_select.
```

So fallback does not necessarily mean "block the whole thread." It means:

```text
Use the existing Ruby IO.select semantics.
```

### Empty IO sets must not return immediately

This is incorrect:

```ruby
return [[], [], []] if readers.empty? && writers.empty?
```

An empty IO set with a timeout represents sleep/timeout behavior, not immediate readiness.

Correct behavior:

```ruby
return wait_without_io(timeout, scheduler) if readers.empty? && writers.empty?
```

If `timeout == nil`, this represents an indefinite wait. When a scheduler provides `kernel_sleep`, the bridge should use:

```ruby
scheduler.kernel_sleep(nil)
```

Otherwise, it should preserve:

```ruby
IO.select([], [], nil, nil)
```

Unit tests should not perform an unbounded real wait. Test `kernel_sleep(nil)` through a fake scheduler, and test fallback semantics without actually blocking indefinitely.

### `to_io` and original object return

`IO.select` can accept IO-like wrapper objects. A scheduler's `io_wait` is safer when called with the actual IO object.

Therefore:

```ruby
wait_io = watched_object.respond_to?(:to_io) ? watched_object.to_io : watched_object
```

But returned ready arrays should contain the original object from `readers` / `writers`, not necessarily the `to_io` target.

This preserves `IO.select`-style caller expectations.

### Read and write on the same IO

The same watched object may appear in both `readers` and `writers`.

This must be treated as one unique watched object with a combined event mask:

```ruby
events = 0
events |= IO::READABLE if readers.include?(watched_object)
events |= IO::WRITABLE if writers.include?(watched_object)
```

Do not use:

```ruby
readers.size + writers.size == 1
```

Use:

```ruby
watched = (readers + writers).uniq
watched.size == 1
```

### `mask == 0`

If `scheduler.io_wait` returns `0`, the bridge should treat it as timeout/no readiness:

```ruby
return [nil, nil, nil] if mask.nil? || mask == false || mask == 0
```

This prevents spin if a scheduler repeatedly returns a zero readiness mask.

### Current scheduler selection

The implementation should prefer `Fiber.current_scheduler` when available, rather than directly using `Fiber.scheduler`.

Direct calls to scheduler hooks should only happen from non-blocking fibers. On older Ruby versions, the implementation may fall back to `Fiber.scheduler` only when the current fiber is not known to be blocking:

```ruby
def current_scheduler
  if Fiber.respond_to?(:current_scheduler)
    Fiber.current_scheduler
  elsif Fiber.respond_to?(:scheduler)
    return nil if Fiber.current.respond_to?(:blocking?) && Fiber.current.blocking?

    Fiber.scheduler
  end
end
```

## Usage

### SSH command execution

```ruby
require "net/ssh"
require "net/ssh/connection/fiber_scheduler_event_loop"

Net::SSH.start(
  host,
  user,
  event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
) do |ssh|
  puts ssh.exec!("echo ok")
end
```

### SFTP

```ruby
require "net/sftp"
require "net/ssh/connection/fiber_scheduler_event_loop"

ssh_options = {
  event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
}

Net::SFTP.start(host, user, ssh_options) do |sftp|
  sftp.upload!("/local/file", "/remote/file")
end
```

### Event loop instance lifetime

A `FiberSchedulerEventLoop` instance is single-use for one SSH session.

Good:

```ruby
hosts.each do |host|
  Net::SSH.start(
    host,
    user,
    event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
  ) do |ssh|
    ssh.exec!("echo ok")
  end
end
```

Bad:

```ruby
event_loop = Net::SSH::Connection::FiberSchedulerEventLoop.new

hosts.each do |host|
  Net::SSH.start(host, user, event_loop: event_loop) do |ssh|
    ssh.exec!("echo ok")
  end
end
```

## Running inside a Fiber scheduler

This proposal is useful when the code runs inside a scheduler-backed non-blocking Fiber.

Example with `async`:

```ruby
require "async"
require "net/ssh"
require "net/ssh/connection/fiber_scheduler_event_loop"

Async do |task|
  task.async do
    Net::SSH.start(
      host,
      user,
      event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
    ) do |ssh|
      puts ssh.exec!("sleep 1 && echo ssh")
    end
  end

  task.async do
    10.times do
      Async::Task.current.sleep(0.1)
      puts "sibling fiber progressed"
    end
  end
end
```

The expected outcome is that the sibling fiber continues to progress while the SSH session waits on socket readiness.

## net-sftp await APIs

The Fiber-friendly behavior primarily comes from the `net-ssh` event loop.

`net-sftp` changes are optional and ergonomic.

Possible additions:

```ruby
class Net::SFTP::Request
  def await
    session.loop { pending? }
    self
  end

  def wait
    await
  end
end
```

```ruby
class Net::SFTP::Session
  def await_open
    loop { opening? }
    self
  end

  def connect!(&block)
    connect(&block)
    await_open
  end
end
```

This allows:

```ruby
request = sftp.write(handle, offset, data)
request.await
```

But this is not required for basic scheduler cooperation. Existing APIs such as `request.wait`, `sftp.write!`, and `sftp.upload!` can already benefit when the underlying SSH session uses the Fiber-friendly event loop.

Upload/download operation objects should also be considered in this layer. `net-sftp` upload/download operations already expose wait-style completion behavior, so await naming may be ergonomic but not fundamental.

## SFTP upload/download considerations

SFTP transfer operations may still involve local file IO.

Even if network readiness is Fiber-friendly, these may remain blocking:

```text
- local file reads during upload
- local file writes during download
- filesystem stat/open/close calls
```

For small files, this may be acceptable. For large files or highly concurrent workloads, consider:

```text
- chunk-level cooperative yielding
- local file IO isolation in worker threads
- Async-compatible file IO wrappers where available
```

## Concurrency model

A single SFTP session should not be freely shared across multiple fibers without coordination.

Shared state includes:

```text
- request IDs
- pending request map
- channel state
- send queue
- protocol negotiation state
```

Applications should serialize access to one SFTP session:

```ruby
semaphore = Async::Semaphore.new(1)

semaphore.async do
  sftp.upload!(local, remote)
end.wait
```

Or use a pool:

```text
one SFTP session per worker
```

The initial recommendation is:

```text
single persistent SFTP session + semaphore serialization
```

## Testing strategy

### net-ssh unit tests

Required cases:

```text
- register rejects reuse across multiple sessions
- single watched object uses scheduler.io_wait
- same object in readers and writers uses combined READABLE | WRITABLE mask
- scheduler.io_wait receives to_io target when watched object responds to to_io
- returned ready arrays contain the original watched object
- io_wait returning READABLE maps to readers
- io_wait returning WRITABLE maps to writers
- io_wait returning READABLE | WRITABLE maps to both
- io_wait returning 0 maps to [nil, nil, nil]
- timeout == 0 uses existing IO.select path
- multiple watched objects use existing IO.select path
- empty IO set with timeout uses kernel_sleep or IO.select fallback
- empty IO set with timeout nil calls scheduler.kernel_sleep(nil) when available
- empty IO set without scheduler preserves existing IO.select semantics without unbounded unit-test waits
- ev_select_and_postprocess dispatches ready IOs to session.ev_do_handle_events
```

### read/write same object test

```ruby
def test_same_object_in_readers_and_writers_uses_combined_mask
  io = FakeIO.new
  scheduler = RecordingScheduler.new
  loop = TestLoop.new
  loop.scheduler = scheduler

  loop.send(:io_select, [io], [io], 1)

  assert_equal IO::READABLE | IO::WRITABLE, scheduler.calls.first.events
end
```

### `to_io` wrapper test

```ruby
class Wrapper
  def initialize(io)
    @io = io
  end

  def to_io
    @io
  end
end
```

Expected:

```text
- scheduler.io_wait receives wrapper.to_io
- io_select returns wrapper in ready arrays
```

### net-sftp tests

```text
- Request#await loops while pending and returns self
- Request#wait preserves existing behavior
- Session#await_open loops while opening and returns self
- connect! delegates to await_open
- wait_for returns response/property after request completion
- upload/download wait APIs continue to work
```

### integration tests

Use a real or containerized SSH/SFTP server.

```ruby
Net::SSH.start(
  host,
  user,
  event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
) do |ssh|
  raise unless ssh.exec!("echo ok").strip == "ok"
end
```

SFTP:

```ruby
Net::SFTP.start(
  host,
  user,
  event_loop: Net::SSH::Connection::FiberSchedulerEventLoop.new
) do |sftp|
  sftp.upload!("/tmp/local.txt", "/tmp/remote.txt")
  raise unless sftp.download!("/tmp/remote.txt") == File.read("/tmp/local.txt")
end
```

Scheduler smoke test:

```text
- run SSH command that waits
- run sibling fiber that ticks every 100ms
- assert sibling fiber progresses during SSH wait
```

## Adoption strategy

### Application-local first

For applications such as `psync`, start with an app-local class.

```text
lib/psync/net_ssh/fiber_scheduler_event_loop.rb
```

Then inject it into SFTP sessions:

```ruby
ssh_options[:event_loop] =
  Net::SSH::Connection::FiberSchedulerEventLoop.new
```

This avoids maintaining forks while validating the design under real workload.

### Library-level later

Only after application-level validation should this become:

```text
- net-ssh upstream PR
- net-sftp ergonomic await API PR
- separate net-ssh-fiber / net-sftp-fiber gems
```

## Risks

### Scheduler variance

Schedulers differ in how they implement `io_select`, `io_wait`, and `kernel_sleep`.

This design reduces variance for the single-IO case but still needs validation against real schedulers such as `async`.

### Direct scheduler hook usage

This proposal sketches direct calls to `scheduler.io_wait`.

That may be debated in upstream review. An implementation based on `IO#wait(events, timeout)` should be evaluated if maintainers prefer using Ruby's public IO API instead of calling scheduler hooks directly.

### Hidden blocking before event loop

The event loop injection begins after the SSH connection/session reaches the connection layer. Earlier phases may still block:

```text
- DNS lookup
- TCP connect
- proxy command setup
- SSH banner exchange
- authentication
```

These are outside this proposal's first milestone.

### Multi-IO features

Port forwarding, listener sockets, and multi-session event loops are not handled by the `io_wait` bridge. They intentionally fall back to `IO.select`.

A future phase may use `scheduler.io_select` explicitly where available.

### Local file IO

SFTP transfer operations may still block on local disk IO. This proposal primarily addresses network readiness waits.

## Recommendation

Proceed in phases.

```text
Phase 1:
  Add application-local FiberSchedulerEventLoop in psync.
  Inject it via event_loop:.
  Validate with persistent SFTP session and semaphore serialization.

Phase 2:
  Add real Async smoke tests.
  Confirm sibling fibers progress during SSH/SFTP waits.
  Confirm no busy loop on timeout/no-IO paths.

Phase 3:
  Decide whether net-ssh upstream PR is worth pursuing.
  Keep net-sftp await API optional and ergonomic.

Phase 4:
  Investigate broader io_select-based support for multi-IO features.
```

## Conclusion

This proposal does not replace Ruby's scheduler-aware `IO.select` path. It adds a conservative, opt-in, single-IO `io_wait` bridge for the most common `net-ssh` / `net-sftp` workload: one SSH session over one transport socket.

The approach keeps existing APIs intact, preserves existing `IO.select` semantics as fallback, avoids broad multi-IO correctness risks, and gives Fiber-based applications a predictable path for SSH/SFTP readiness waits.
