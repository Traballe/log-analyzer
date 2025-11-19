# LogAnalyzer
LogAnalyzer is a Python-based tool designed to analyze log files using the Drain3 algorithm for log parsing and template mining. It processes log entries, identifies patterns, and generates templates to help understand and manage log data effectively.

## Usage
If starting the python script from the command line, the following usage is used:
```ps1
usage: logAnalyzer.py [-h] [-o [OUTPUT_DIR]] [-v] path_to_log_file

Sysparency (R) Log Analyser Tool

positional arguments:
  path_to_log_file      The path to the log file to analyze. (example: "C:/reports/demo.log")

options:
  -h, --help            show this help message and exit
  -o, --output_dir [OUTPUT_DIR]
                        The directory to store output files (default: "./output")
  -v, --verbosity       Verbosity level. Use -v for INFO, -vv for DEBUG (default: WARNING)
```
Note: Paths can be absolute or relative and support both forward slashes (/) and backslashes (\\) as directory separators.

You can also build a single executable using PyInstaller:
```ps1
pyinstaller --onefile logAnalyzer.py
```
and then run the resulting `logAnalyzer.exe` with the same arguments as above.

## Currently Supported Log Formats
The tool is currently configured to handle log files formatted with Java's SimpleFormatter. In this format, each log entry starts with a date and time stamp (e.g., "Mar. 22, 2025 3:11:31 PM"), followed by the log level, message, optional stack trace lines and supports multiline log entries.
The standard pattern used to split log messages is based on `start_datetime_pattern = r'^[A-Z][a-z]{2}\. \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M'`, which matches date and time formats like "Mar. 22, 2025 3:11:31 PM".
If you have a different log format, you may need to adjust the regular expression in the `drain_from_file.py` file accordingly.