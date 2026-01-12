import argparse
import signal
import logging
import sys
import time
from pathlib import Path
from .node import TrafficLightNode


def main():
    parser = argparse.ArgumentParser(description="Distributed Traffic Light Node")
    parser.add_argument("node_name", type=str, help="Node name (any string)")
    parser.add_argument("node_id", type=int, help="Node ID (higher = higher priority)")
    args = parser.parse_args()

    # ----------------------------
    # Logging: console + log/ folder
    # ----------------------------
    log_dir = Path.cwd() / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()  # prevent duplicate logs if main() is run multiple times

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))

    file_path = log_dir / f"traffic_light_{args.node_name}_{args.node_id}.log"
    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))

    root.addHandler(console_handler)
    root.addHandler(file_handler)

    logging.getLogger(__name__).info(f"Logging to file: {file_path}")

    # Initialize and start the traffic light node
    node = TrafficLightNode(args.node_name, args.node_id)
    node.start()

    # Handle graceful shutdown on Ctrl+C
    def shutdown(signum, frame):
        logging.info("Shutting down node...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    # Periodically log/print node status
    try:
        while node._running:
            time.sleep(5)
            print(f"Node {node.node_name}: Phase={node.current_phase}, Leader={node.election.leader_name}")
    except Exception:
        node.stop()


if __name__ == "__main__":
    main()
