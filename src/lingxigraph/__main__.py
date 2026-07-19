"""Enable ``python -m lingxigraph`` as an alias for the console entry point."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
