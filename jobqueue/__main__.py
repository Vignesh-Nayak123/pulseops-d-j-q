"""Package entrypoint.

Usage:
  python -m jobqueue worker   run a worker process (claim/run/ack loop).
  python -m jobqueue api      run the Flask API with the dev server.

In production the API is served by gunicorn (see docker-compose.yml) which
imports jobqueue.api:app directly. The "api" subcommand here is for local runs.
"""

import sys


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print("usage: python -m jobqueue [worker|api]", file=sys.stderr)
        return 2

    command = argv[0]
    if command == "worker":
        from .worker import run
        run()
    elif command == "api":
        import os
        from .api import app
        # host 0.0.0.0 so the port is reachable from outside the container.
        port = int(os.environ.get("API_PORT", 8000))
        app.run(host="0.0.0.0", port=port)
    else:
        print("unknown command %r, expected 'worker' or 'api'" % command, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
