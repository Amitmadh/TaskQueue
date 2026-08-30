"""Allow 'python -m TaskQueue ...' as an alternative to the 'taskqueue' script.

Useful when a parent process needs the worker to be its *direct* child: on
Windows the installed 'taskqueue.exe' is a launcher stub that runs the
interpreter as a separate child process, so a Popen handle on the stub cannot
signal the worker itself.
"""

from TaskQueue.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
