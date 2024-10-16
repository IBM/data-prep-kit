import abc
import datetime
import fcntl
import os
import tempfile
import threading
import time


_tempdir = tempfile.gettempdir()


class MultiLock(abc.ABC):
    """
    Provides a process- and thread-locked lock.
    To use
        lock = MultiLock("mylock")
        ...
        lock.acquire(block=false, timeout=30)
        # do something critical
        ...
        lock.release()
    """

    def __init__(self, name):
        """
        Create the lock with the given name.

        :param name: the global name associated with this lock. All processes using the same
            name will be part of the same locking cohort.  It is up to the caller to define
            and coordinate lock names.
        """
        if name is None or len(name) == 0:
            raise ValueError("lock name must not be None or the empty string")
        self.lock_filename = os.path.join(_tempdir, name + ".multilock")
        # print(f"lock file name is {self.lock_filename}")
        self.fd = None
        self.thread_lock = threading.Lock()

    def acquire(self, block=True, timeout=None):
        """
        With the block argument set to True (the default), the method call will block until the
        lock is in an unlocked state, then set it to locked and return True.

        With the block argument set to False, the method call does not block. If the lock
        is currently in a locked state, return False; otherwise set the lock to a locked state and return True.

        When invoked with a positive, floating-point value for timeout, wait for at most the number
        of seconds specified by timeout as long as the lock can not be acquired. Invocations with a
        negative value for timeout are equivalent to a timeout of zero. Invocations with a timeout
        value of None (the default) set the timeout period to infinite. The timeout argument has no practical
        implications if the block argument is set to False and is thus ignored.

        Returns True if the lock has been acquired or False if the timeout period has elapsed.

        """
        if self.fd is not None:  # Already locked.
            return True

        start = time.time()
        if block:
            locked = self.thread_lock.acquire(block, timeout)
        else:
            locked = self.thread_lock.acquire(block)
        if not locked:
            return False
        end = time.time()
        if not block and timeout > 0:
            timeout -= end - start
            if timeout <= 0:
                self.thread_lock.release()
                return False

        # open a file and create a file descriptor
        self.fd = os.open(self.lock_filename, os.O_RDWR | os.O_CREAT)

        msg = f"MultiLock last held by process with pid={os.getpid()}\n"
        os.write(self.fd, str.encode(msg))

        # put a lock on an open file
        locked = False
        waited = 0
        sleep_seconds = 1
        if timeout is not None:
            timeout = max(0, timeout)
        while not locked and (timeout is None or waited <= timeout):
            try:
                fcntl.lockf(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except Exception as exc:
                # Only get here if lock could not be acquired
                # print(f"sleeping {exc=}")
                time.sleep(sleep_seconds)
                if not block:
                    break
                waited += sleep_seconds
        if not locked:
            # If we didn't get the lock, then release the file.
            os.close(self.fd)
            self.fd = None

        self.thread_lock.release()
        return locked

    def release(self):
        """
        Release an acquired lock.  Do nothing if the lock is not acquired.
        :return:
        """
        if self.fd is not None:
            self.thread_lock.acquire()
            os.close(self.fd)
            self.fd = None
            self.thread_lock.release()

    def is_locked(self):
        return self.fd is not None


def main(block, timeout, sleep):
    lock = MultiLock("foo")
    if block:
        print(f"going to acquire the blocking lock with timeout={timeout}")
    else:
        print(f"going to acquire the non-blocking lock with timemout={timeout}")
    locked = lock.acquire(block=block, timeout=timeout)
    start = datetime.datetime.now()
    start = start.strftime("%Y-%m-%d %H:%M:%S")
    if not locked:
        print(f"Could not get lock at {start}")
        return
    print(f"{start}: I got the lock")
    time.sleep(sleep)
    lock.release()
    end = datetime.datetime.now()
    end = end.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{end}: lock released")
    print(f"lock held from {start} to {end}")


if __name__ == "__main__":
    sleep = 10
    timeout = 10
    main(True, timeout, sleep)
    time.sleep(1)
    main(False, timeout, sleep)
