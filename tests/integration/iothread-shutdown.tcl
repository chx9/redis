#
# Copyright (c) 2025-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of (a) the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#

# Tests that IO threads shut down cleanly (no deadlock) when the server exits.
#
# Background
# ----------
# killIOThreads() originally called pthread_cancel() to stop IO threads.
# pthread_cancel() with PTHREAD_CANCEL_ASYNCHRONOUS (set by makeThreadKillable)
# can fire at any point, including while the thread holds a glibc malloc /
# jemalloc tcache internal lock during arena shutdown.  When that happens,
# pthread_join() blocks forever because the thread is stuck inside the
# allocator with the lock held and can never reach a cancellation point.
#
# The fix replaces "cancel + join" with a cooperative-stop approach:
#   Step 1: aeStop() sets the event-loop stop flag for each IO thread.
#   Step 2: triggerEventNotifier() wakes the thread if blocked in epoll_wait.
#           The write() syscall provides the necessary memory barrier so the
#           IO thread sees the updated stop flag when it wakes.
#   Step 3: pthread_join() waits for the thread to exit naturally – no
#           pthread_cancel() involved, eliminating the allocator-lock deadlock.
#
# How to reproduce the bug without the fix
# ----------------------------------------
# Without the fix the server may hang indefinitely in killIOThreads() during
# shutdown.  The tests below detect this by asserting that the server process
# disappears within a generous but finite timeout after SHUTDOWN NOSAVE is
# issued while IO threads are active.
#
# With the fix the server exits promptly and the tests pass.

# ---------------------------------------------------------------------------
# Helper: wait for a process to disappear within $max_ms milliseconds.
# Returns 1 if process exited within the deadline, 0 otherwise.
# ---------------------------------------------------------------------------
proc wait_pid_exit {pid max_ms} {
    set deadline [expr {[clock milliseconds] + $max_ms}]
    while {[clock milliseconds] < $deadline} {
        if {![is_alive $pid]} {
            return 1
        }
        after 50
    }
    return 0
}

# ---------------------------------------------------------------------------
# Test 1: server with IO threads shuts down cleanly under idle conditions.
# Without the fix: pthread_join hangs forever → process never exits → FAIL.
# With the fix:    IO threads exit naturally  → process exits in ms   → PASS.
# ---------------------------------------------------------------------------
start_server {overrides {io-threads 4 save ""} tags {"iothreads external:skip"}} {
    test {IO threads: server shuts down cleanly (no deadlock) under idle load} {
        set pid [s process_id]
        set log [srv 0 stdout]

        # Issue SHUTDOWN NOSAVE – connection drops immediately, so catch the error.
        catch {r shutdown nosave}

        # The server must disappear within 10 seconds.
        # Without the cooperative-stop fix the process hangs here indefinitely.
        set exited [wait_pid_exit $pid 10000]
        if {!$exited} {
            fail "Server did not exit within 10s – possible deadlock in killIOThreads()"
        }

        # Verify the log contains "IO thread … terminated" emitted after a
        # successful pthread_join() in the fixed code.
        assert {[count_message_lines $log "IO thread"] > 0}
        assert_match "*IO thread*terminated*" [exec cat $log]
    }
}

# ---------------------------------------------------------------------------
# Test 2: server with IO threads shuts down cleanly while clients are active.
# This stresses the allocator path: IO threads are actively doing malloc/free
# when the shutdown races in, which is the exact scenario that triggered the
# original deadlock.
# ---------------------------------------------------------------------------
start_server {overrides {io-threads 4 save ""} tags {"iothreads external:skip"}} {
    test {IO threads: server shuts down cleanly under active client load} {
        set pid  [s process_id]
        set host [srv 0 host]
        set port [srv 0 port]
        set log  [srv 0 stdout]

        # Populate some data so IO threads have real work.
        for {set i 0} {$i < 200} {incr i} {
            r set "key:$i" "value:$i"
        }

        # Start a background write-load process using the standard Redis helper.
        # Run for 30 seconds (more than enough; we'll shut the server down first).
        set load_handle [start_write_load $host $port 30]

        # Give the background writer a moment to start sending.
        after 300

        # Shut down while the writer is active.
        catch {r shutdown nosave}

        # Server must exit within 10 seconds even under load.
        set exited [wait_pid_exit $pid 10000]

        # Clean up background load process regardless of outcome.
        stop_write_load $load_handle

        if {!$exited} {
            fail "Server did not exit within 10s under load – possible deadlock in killIOThreads()"
        }

        # Verify IO threads were joined cleanly.
        assert_match "*IO thread*terminated*" [exec cat $log]
    }
}

# ---------------------------------------------------------------------------
# Test 3: repeated restart – each cycle must complete within the time budget.
# restart_server calls kill_server which sends SIGTERM and then waits up to
# 10 s (or 120 s under valgrind) before force-killing with SIGKILL.
# If killIOThreads() deadlocks, kill_server times out and force-kills with
# SIGSEGV then SIGKILL, and the test framework reports an error.
# ---------------------------------------------------------------------------
start_server {overrides {io-threads 4 save ""} tags {"iothreads external:skip"}} {
    test {IO threads: server can be restarted without hanging in killIOThreads} {
        set log [srv 0 stdout]

        # Do some work so IO threads are exercised.
        for {set i 0} {$i < 50} {incr i} {
            r set "k$i" "v$i"
        }

        # restart_server kills the old server (SIGTERM) and starts a new one.
        # rotate_logs=false keeps the old log in the same file so we can check it.
        # If killIOThreads deadlocks, kill_server times out and force-kills.
        restart_server 0 true false

        # After restart the server must respond normally.
        assert_equal "PONG" [r ping]

        # The log (which includes output from before the restart since we did
        # not rotate) must show clean IO-thread termination.
        assert_match "*IO thread*terminated*" [exec cat $log]
    }
}
