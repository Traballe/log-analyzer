# SPDX-License-Identifier: MIT

import json
import logging
import os
from pathlib import Path
import re
import sys
from os.path import dirname

from tqdm import tqdm

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# persistence_type = "NONE"
# persistence_type = "REDIS"
# persistence_type = "KAFKA"
persistence_type = "FILE"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.WARNING, format='%(message)s')
verbosity = "warning"
output_dir = "./output"

def main(argv: list):
    global verbosity
    global output_dir
    log_file = None
    # check if argv has a tuple where the first element is 'path_to_log_file'
    for arg in argv:
        if arg[0] == 'path_to_log_file':
            log_file = Path(arg[1])
        elif arg[0] == 'verbosity':
            verbosity = arg[1]
            if verbosity == "debug":
                logging.getLogger().setLevel(logging.DEBUG)
                logger.debug("Debug logging enabled")
            elif verbosity == "info":
                logging.getLogger().setLevel(logging.INFO)
                logger.info("Info logging enabled")
            elif verbosity == "warning":
                logging.getLogger().setLevel(logging.WARNING)
            elif verbosity == "error":
                logging.getLogger().setLevel(logging.ERROR)
        elif arg[0] == 'output_dir':
            output_dir = Path(arg[1])
            os.makedirs(output_dir, exist_ok=True)
    if log_file is None:
        sys.exit(f"Usage: python {sys.argv[0]} <path_to_log_file>\n")

    log_name = os.path.splitext(os.path.basename(log_file))[0]
    if persistence_type == "KAFKA":
        from drain3.kafka_persistence import KafkaPersistence

        persistence = KafkaPersistence(f"{log_name}_state", bootstrap_servers="localhost:9092")
    elif persistence_type == "FILE":
        from drain3.file_persistence import FilePersistence

        persistence = FilePersistence(f"{output_dir}/{log_name}_state.json")
    elif persistence_type == "REDIS":
        from drain3.redis_persistence import RedisPersistence

        persistence = RedisPersistence(redis_host='',
                                    redis_port=25061,
                                    redis_db=0,
                                    redis_pass='',
                                    is_ssl=True,
                                    redis_key=f"{log_name}_state_key")
    else:
        raise ValueError(f"Unsupported persistence type '{persistence_type}'")

    config = TemplateMinerConfig()
    config.load(f"{dirname(__file__)}/drain3.ini")
    config.profiling_enabled = False
    config.snapshot_compress_state = False

    template_miner = TemplateMiner(None, config) # pyright: ignore[reportArgumentType]
    # setting persistence after TemplateMiner creation to avoid loading state but still persisting ("standard" persistence could be used to give predefined patterns)
    template_miner.persistence_handler = persistence
    print("\n\n")
    title = f"Sysparency (R) Log Analyzer with Drain3 algorithm started"
    print((len(title)+4) * "*")
    print(f"* {title} *")
    print((len(title)+4) * "*")
    print("\n")
    print("Log file to analyze:", log_file)
    print(f"Configuration loaded from './drain3.ini'")
    print(f"Persistence type: {persistence_type}")
    if verbosity == "debug":
        print(f"{len(config.masking_instructions)} masking instructions are in use")

    print()
    messages = list(read_log_messages(log_file))
    train_from_file(template_miner, messages)
    if verbosity == "debug":
        print("Training done. Mined clusters:")
        for cluster in template_miner.drain.clusters:
            print(cluster)

    run_on_file(template_miner, messages)

    log_file = Path(output_dir, log_name + "_clusters.log")
    save_cluster_results(template_miner, log_file)
    print(f"- {Path(output_dir, log_name + "_clusters.log")}  <- Summary")
    print(f"- {Path(output_dir, log_name + "_state.json")}    <- Persisted Drain3 state")
    print("\nLog analysis completed.")

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

    # print("\nReading Logs")
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

def train_from_file(template_miner: TemplateMiner, messages: list):
    # print("\nDeveloping Clusters")
    for message in tqdm(messages, desc="Developing Clusters".ljust(23)):
        result = template_miner.add_log_message(message)
        if verbosity == "debug":
            result_json = json.dumps(result)
            print(result_json)
            template = result["template_mined"]
            params = template_miner.extract_parameters(template, message)
            print(f"Parameters: {str(params)}")

def run_on_file(template_miner: TemplateMiner, messages: list):
    # print("\nInferencing")
    for message in tqdm(messages, desc="Inferencing".ljust(23)):
        cluster = template_miner.match(message)
        if verbosity == "debug":
            if cluster is None:
                print(f"No match found")
            else:
                template = cluster.get_template()
                print(f"Matched template #{cluster.cluster_id}: {template}")
                print(f"Parameters: {template_miner.get_parameter_list(template, message)}")

def save_cluster_results(template_miner: TemplateMiner, path_to_log_file: Path):
    
    # print("\nStoring Cluster Results")
    log_file_name = os.path.splitext(os.path.basename(path_to_log_file))[0]

    sorted_clusters = sorted(template_miner.drain.clusters, key=lambda c: c.size, reverse=True)
    with open(path_to_log_file, "w", encoding="utf-8") as f:
        for cluster in tqdm(sorted_clusters, desc="Storing Cluster Results"):
            print(cluster, file=f)

if __name__ == "__main__":
    main([("path_to_log_file", "C:/reports/demo.log"), ("output_dir", "./output"), ("verbosity", "warning")])