"""Optional standalone helper bundled as a readable Agent Skill resource."""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("name", nargs="?", default="friend")
    options = parser.parse_args()
    print(f"Hello, {options.name}!")


if __name__ == "__main__":
    main()
