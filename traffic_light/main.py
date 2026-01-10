import argparse
import signal
import logging
import sys
import time
from .node import TrafficLightNode

def main():
    parser = argparse.ArgumentParser(description='Distributed Traffic Light Node')
    parser.add_argument('node_name', type=str, help='Node name (any string)')
    parser.add_argument('node_id', type=int, help='Node ID (higher = higher priority)')
    args = parser.parse_args()
    # Configure logging to console and file
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    file_handler = logging.FileHandler(f"traffic_light_{args.node_name}_{args.node_id}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(file_handler)
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
        # If any unexpected error happens, ensure node is stopped
        node.stop()

if __name__ == '__main__':
    main()