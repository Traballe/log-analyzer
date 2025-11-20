"""Sysparency Log Analyzer CLI entry point.

Provides the `main()` function that parses command-line arguments and
delegates log analysis to `drain_from_file.main(args)`.
"""

import argparse
import drain_from_file

def main():
    """
    Entry point for the Sysparency Log Analyzer CLI.

    This function builds and parses command-line arguments for running the log
    analysis and then delegates execution to drain_from_file.main(args).

    Arguments parsed:
    - path_to_log_file (str, positional)
        Path to the log file to analyze (e.g. "C:/reports/demo.log").
    - -o / --output_dir (str, optional; default="./output")
        Directory where output files will be written.
    - -v / --verbosity (count, optional)
        Increase logging verbosity. Use:
          - no flag: WARNING level
          - -v: INFO level
          - -vv (or more): DEBUG level
        The parsed numeric verbosity count is converted to a string value:
        "warning", "info", or "debug" before being passed to the downstream
        handler.

    Behavior and side effects:
    - Uses argparse to parse command-line arguments; invalid arguments will cause
      argparse to emit an error and exit.
    - Mutates args.verbosity from an integer count to a string level.
    - Calls drain_from_file.main(args) to perform the actual analysis. Any exceptions
      raised by that call propagate to the caller.

    Returns:
    - None

    Raises:
    - SystemExit when argparse encounters parse errors or when help/version is requested.
    - Any exception raised by drain_from_file.main may be propagated.

    Example:
        $ python log_analyzer C:/reports/demo.log -o ./out -vv
    """
    # 1. Create the ArgumentParser object
    parser = argparse.ArgumentParser(
        description="Sysparency (R) Log Analyzer Tool"
    )

    # 2. Define the arguments the program will accept
    # A required positional argument
    parser.add_argument(
        "path_to_log_file",
        type=str,
        help="The path to the log file to analyze. (example: \"C:/reports/demo.log\")"
    )

    # An optional argument
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        nargs="?",
        default="./output",
        help="The directory to store output files (default: \"./output\")"
    )

    # An optional argument
    parser.add_argument(
        "-v", 
        "--verbosity",
        action="count", default=0,
        help="Verbosity level. Use -v for INFO, -vv for DEBUG (default: WARNING)"
    )



    args = parser.parse_args()
    args.verbosity = "debug" if args.verbosity > 1 else "info" if args.verbosity > 0 else "warning"

    drain_from_file.main(args)

if __name__ == "__main__":
    main()
