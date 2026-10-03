"""Show per-file min/max of one column at chosen Silver versions (read-only, no Spark).

Uses only the _delta_log statistics, e.g. baseline v46 vs Z-ORDER v48:

    python -m scripts.show_file_stats --versions 46 48 --value 132
"""

from __future__ import annotations

import argparse

from src.common.config import SILVER_PATH
from src.optimization.data_skipping import active_files, column_range, files_to_read


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default=str(SILVER_PATH))
    parser.add_argument("--versions", type=int, nargs="+", required=True)
    parser.add_argument("--column", default="PULocationID")
    parser.add_argument("--value", type=int, help="Also count files a filter column = value reads")
    parser.add_argument("--limit", type=int, default=20, help="Files listed per version")
    args = parser.parse_args(argv)

    for version in args.versions:
        files = active_files(args.path, version)
        ranges = [(column_range(stats, args.column), path) for path, stats in files.items()]
        # Sort by (min, max); files without stats for the column are listed last.
        ranges.sort(key=lambda item: (item[0] is None, item[0] or (0, 0), item[1]))
        print(f"\n=== version {version}: {len(files)} files ===")
        print(f"{'min':>8} {'max':>8}  file")
        for bounds, path in ranges[: args.limit]:
            low, high = bounds or ("-", "-")
            print(f"{low!s:>8} {high!s:>8}  {path[:70]}")
        if len(ranges) > args.limit:
            print(f"... {len(ranges) - args.limit} more files")
        if args.value is not None:
            needed = files_to_read(files, args.column, (args.value, args.value))
            print(f"{args.column} = {args.value}: {needed}/{len(files)} files to read")


if __name__ == "__main__":
    main()
