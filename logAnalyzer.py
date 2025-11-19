import argparse
import drain_from_file

def main():
    # 1. Create the ArgumentParser object
    parser = argparse.ArgumentParser(
        description="Sysparency (R) Log Analyser Tool"
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

    drain_from_file.main(args._get_kwargs())

if __name__ == "__main__":
    main()
