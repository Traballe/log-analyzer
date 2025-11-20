# SPDX-License-Identifier: MIT

"""Sysparency Log Analyzer CLI logic.

This module reads a file containing log messages, parses them, applies the Drain3
algorithm to mine log templates, and writes the results to output files.
"""

from argparse import Namespace
import json
import logging
import os
from pathlib import Path
import re
import sys
from os.path import dirname
from typing import Optional, Sequence, Tuple, Union

from tqdm import tqdm

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.kafka_persistence import KafkaPersistence
from drain3.redis_persistence import RedisPersistence
from drain3.template_miner_config import TemplateMinerConfig

# persistence_type = "NONE"
# persistence_type = "REDIS"
# persistence_type = "KAFKA"
PERSISTENCE_TYPE = "FILE"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.WARNING, format='%(message)s')
DEFAULT_VERBOSITY = "warning"
DEFAULT_OUTPUT_DIR = Path("./output")

def main(argv: Namespace) -> None:
    """Run the Drain3-based log analysis using CLI arguments."""

    script_name = Path(sys.argv[0]).name
    log_file = _resolve_log_file(getattr(argv, "path_to_log_file", None), script_name)
    verbosity = getattr(argv, "verbosity", None) or DEFAULT_VERBOSITY
    output_dir = _resolve_output_dir(getattr(argv, "output_dir", None))

    _configure_logging(verbosity)

    log_name = log_file.stem
    persistence = _build_persistence_handler(log_name, output_dir)
    template_miner, config = _create_template_miner(persistence)

    _print_banner(log_file, config, verbosity)

    messages = list(read_log_messages(log_file))
    train_from_file(template_miner, messages, verbosity)
    if verbosity == "debug":
        print("Training done. Mined clusters:")
        for cluster in template_miner.drain.clusters:
            print(cluster)

    run_on_file(template_miner, messages, verbosity)

    cluster_log_file = output_dir / f"{log_name}_clusters.log"
    state_file = output_dir / f"{log_name}_state.json"
    save_cluster_results(template_miner, cluster_log_file)
    print(f"- {cluster_log_file}  <- Summary")
    print(f"- {state_file}    <- Persisted Drain3 state")
    print("\nLog analysis completed.")


def _resolve_log_file(path_value: Optional[Union[str, Path]], script_name: str) -> Path:
    """Validate that the log file argument is provided and exists."""

    if not path_value:
        sys.exit(f"Usage: python {script_name} <path_to_log_file>\n")

    log_file = Path(path_value)
    if not log_file.exists():
        sys.exit(f"Log file '{log_file}' does not exist.\n")

    return log_file


def _configure_logging(level: str) -> None:
    """Configure root logger according to the requested verbosity."""

    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    logging.getLogger().setLevel(level_map.get(level, logging.WARNING))

    if level == "debug":
        logger.debug("Debug logging enabled")
    elif level == "info":
        logger.info("Info logging enabled")


def _resolve_output_dir(path_value: Optional[Union[str, Path]]) -> Path:
    """Return an existing output directory, creating it when needed."""

    target_dir = Path(path_value) if path_value else DEFAULT_OUTPUT_DIR
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


def _build_persistence_handler(log_name: str, output_dir: Path):
    """Instantiate the configured persistence handler for Drain3."""

    if PERSISTENCE_TYPE == "KAFKA":
        return KafkaPersistence(f"{log_name}_state", bootstrap_servers="localhost:9092")
    if PERSISTENCE_TYPE == "FILE":
        return FilePersistence(str(output_dir / f"{log_name}_state.json"))
    if PERSISTENCE_TYPE == "REDIS":
        return RedisPersistence(
            redis_host='',
            redis_port=25061,
            redis_db=0,
            redis_pass='',
            is_ssl=True,
            redis_key=f"{log_name}_state_key",
        )

    raise ValueError(f"Unsupported persistence type '{PERSISTENCE_TYPE}'")


def _create_template_miner(persistence_handler) -> Tuple[TemplateMiner, TemplateMinerConfig]:
    """Create a TemplateMiner configured for this CLI."""

    config = TemplateMinerConfig()
    config.load(f"{dirname(__file__)}/drain3.ini")
    config.profiling_enabled = False
    config.snapshot_compress_state = False

    template_miner = TemplateMiner(None, config)  # pyright: ignore[reportArgumentType]
    template_miner.persistence_handler = persistence_handler
    return template_miner, config


def _print_banner(log_file: Path, config: TemplateMinerConfig, verbosity: str) -> None:
    """Emit a short run header describing the current configuration."""

    print("\n\n")
    title = "Sysparency (R) Log Analyzer with Drain3 algorithm started"
    print((len(title) + 4) * "*")
    print(f"* {title} *")
    print((len(title) + 4) * "*")
    print("\n")
    print("Log file to analyze:", log_file)
    print("Configuration loaded from './drain3.ini'")
    print(f"Persistence type: {PERSISTENCE_TYPE}")
    if verbosity == "debug":
        print(f"{len(config.masking_instructions)} masking instructions are in use")

def read_log_messages(path_to_log_file: Path):
    """
    Yield complete log messages from a file. Supports java.util.logging.SimpleFormatter two-line
    output where the first line contains the timestamp (e.g.
    "Mar. 22, 2025 3:11:31 PM") and source. The second line contains the level and
    message (e.g. "SEVERE: message") followed by optional stacktrace lines.
    """
    start_datetime_pattern = r'^[A-Z][a-z]{2}\. \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M'
    start_re = re.compile(start_datetime_pattern + r' [\S\s]+')
    buffer = []

    with open(path_to_log_file, 'r', encoding='utf-8', errors='replace') as f:
        total_lines = sum(1 for line in f)
    with open(path_to_log_file, 'r', encoding='utf-8', errors='replace') as f:
        for raw in tqdm(f, total=total_lines, desc="Processing logs".ljust(23)):
            line = raw.rstrip('\n')
            if not line.strip():
                # blank line separates messages; flush buffer if present
                if buffer:
                    yield '\n'.join(buffer)
                    buffer = []
                continue
            if start_re.match(line):
                # start of a new Java SimpleFormatter message
                if buffer:
                    yield '\n'.join(buffer)
                # replace date/time with "" in line
                line = re.sub(start_datetime_pattern, "", line)
                buffer = [line]
            else:
                # continuation line (level/message or stacktrace), attach to buffer
                if buffer:
                    buffer.append(line)
                else:
                    # no buffer yet (a non-timestamp line at file start) - treat as standalone
                    buffer = [line]
        if buffer:
            yield '\n'.join(buffer)

def train_from_file(template_miner: TemplateMiner, messages: Sequence[str], verbosity: str) -> None:
    """Feed all log messages into the template miner for training."""

    for message in tqdm(messages, desc="Developing Clusters".ljust(23)):
        result = template_miner.add_log_message(message)
        if verbosity == "debug":
            result_json = json.dumps(result)
            print(result_json)
            template = result["template_mined"]
            params = template_miner.extract_parameters(template, message)
            print(f"Parameters: {str(params)}")

def run_on_file(template_miner: TemplateMiner, messages: Sequence[str], verbosity: str) -> None:
    """Match log messages against the mined clusters."""

    for message in tqdm(messages, desc="Inferencing".ljust(23)):
        cluster = template_miner.match(message)
        if verbosity == "debug":
            if cluster is None:
                print("No match found")
            else:
                template = cluster.get_template()
                print(f"Matched template #{cluster.cluster_id}: {template}")
                print(f"Parameters: {template_miner.get_parameter_list(template, message)}")

def save_cluster_results(template_miner: TemplateMiner, output_path: Path) -> None:
    """Persist mined cluster summaries to the requested file."""

    sorted_clusters = sorted(template_miner.drain.clusters, key=lambda c: c.size, reverse=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for cluster in tqdm(sorted_clusters, desc="Storing Cluster Results"):
            print(cluster, file=f)

if __name__ == "__main__":
    main(Namespace(
        path_to_log_file="C:/reports/demo.log",
        output_dir="./output",
        verbosity="warning"
    ))
