import logging
import threading
import time
import random
import socket


def thread_exception_handler(args):
    logging.error("Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


class Raft:
    def __init__(self, node_id: int, port: int, neighbors_ports: list, lb_fault_duration: int, is_continue: bool,
                 heartbeat_duration: float):
        logging.info("Initialise port and node dictionary...")
        self.node_id = node_id
        self.port = port
        self.neighbors_ports = neighbors_ports
        self.heartbeat_duration = heartbeat_duration
        self.lb_fault_duration = lb_fault_duration
        self.is_continue = is_continue

        logging.info("Initialise persistent variables...")
        self.current_term = 0
        self.voted_for = None

        logging.info("Initialise volatile state...")
        self.current_role = 'follower'  # Possible roles: leader, candidate, follower
        self.current_leader = None
        self.votes_received = set()

        logging.info("Initialise flag variable...")
        self.stop_event = threading.Event()

        logging.info("Initialise election timer thread...")
        self.election_timeout = None
        self.reset_election_timeout()

        logging.info("Creating socket and binding...")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", port))
        self.socket.settimeout(0.5)  # Non-blocking with timeout

    def reset_election_timeout(self):
        if self.election_timeout is not None:
            logging.info("Cancelling existing election timeout...")
            self.election_timeout.cancel()

        timeout = self.lb_fault_duration + random.uniform(0, 2)
        logging.info(f"Setting election timeout duration: {timeout}s")
        self.election_timeout = threading.Timer(timeout, self.start_election)
        self.election_timeout.start()

    def start_election(self):
        logging.info("Election timer triggered...")
        self.current_term += 1
        self.voted_for = self.node_id
        self.current_role = 'candidate'
        self.votes_received = {self.node_id}
        logging.info(f"Node {self.node_id} transitions to 'candidate' for term {self.current_term}. Starting election.")

        for port in self.neighbors_ports:
            self.send_vote_request(port)

        self.reset_election_timeout()

    def send_vote_request(self, neighbor_port):
        logging.info(f"Node {self.node_id} sending vote request to port {neighbor_port} for term {self.current_term}")
        message = f"VoteRequest#{self.node_id}#{self.current_term}"
        self.socket.sendto(message.encode(), ("127.0.0.1", neighbor_port))

    def listen_messages(self):
        logging.info(f"Node {self.node_id} starting to listen for messages...")
        while not self.stop_event.is_set():
            try:
                data, addr = self.socket.recvfrom(1024)
                message = data.decode()
                parts = message.split("#")
                logging.info(f"Node {self.node_id} received message from {addr}: {message}")

                if parts[0] == "VoteRequest":
                    if len(parts) == 3:  # Correct length for VoteRequest
                        _, sender_id, sender_term = parts
                        sender_id = int(sender_id)
                        sender_term = int(sender_term)
                        self.handle_vote_request(sender_id, sender_term)
                    else:
                        logging.error("Invalid VoteRequest message format.")
                elif parts[0] == "VoteResponse":
                    if len(parts) == 4:  # Correct length for VoteResponse, includes vote_granted
                        _, sender_id, sender_term, vote_granted = parts
                        sender_id = int(sender_id)
                        sender_term = int(sender_term)
                        vote_granted = vote_granted == "True"
                        self.handle_vote_response(sender_id, sender_term, vote_granted)
                    else:
                        logging.error("Invalid VoteResponse message format.")
                elif parts[0] == "Heartbeat":
                    if len(parts) == 3:  # Correct length for Heartbeat
                        _, sender_id, sender_term = parts
                        sender_id = int(sender_id)
                        sender_term = int(sender_term)
                        self.handle_heartbeat(sender_id, sender_term)
                    else:
                        logging.error("Invalid Heartbeat message format.")
                else:
                    logging.error(f"Unknown message type: {parts[0]}")
            except socket.timeout:
                continue

    def handle_vote_request(self, sender_id, sender_term):
        logging.info(f"Node {self.node_id} handling vote request from node {sender_id} for term {sender_term}")

        # Update term if sender's term is higher
        if sender_term > self.current_term:
            logging.info(
                f"Node {self.node_id}: New term {sender_term} detected, updating from term {self.current_term}")
            self.current_term = sender_term
            self.voted_for = None
            self.current_role = 'follower'

        # Vote for the sender if haven't voted for anyone else or if the sender is the one we voted for
        if self.current_term == sender_term and (self.voted_for is None or self.voted_for == sender_id):
            logging.info(f"Node {self.node_id} votes for node {sender_id} for term {sender_term}")
            self.voted_for = sender_id
            self.send_vote_response(sender_id, self.current_term, True)
            # Reset the election timeout since we've acknowledged a candidate
            self.reset_election_timeout()
        else:
            logging.info(f"Node {self.node_id} denies vote for node {sender_id} for term {sender_term}")
            self.send_vote_response(sender_id, self.current_term, False)

    def send_vote_response(self, receiver_id, term, vote_granted):
        response = "granted" if vote_granted else "denied"
        logging.info(f"Node {self.node_id} sending vote {response} response to node {receiver_id} for term {term}")
        message = f"VoteResponse#{self.node_id}#{term}#{vote_granted}"
        receiver_port = self.neighbors_ports[receiver_id - 1]
        self.socket.sendto(message.encode(), ("127.0.0.1", receiver_port))

    def handle_vote_response(self, sender_id, sender_term, vote_granted):
        logging.info(
            f"Node {self.node_id} received vote response from node {sender_id} for term {sender_term}: {vote_granted}")

        # Ensure the response is for the current term and we are a candidate
        if sender_term == self.current_term and self.current_role == 'candidate' and vote_granted:
            self.votes_received.add(sender_id)
            # Check if we have a majority
            if len(self.votes_received) > len(self.neighbors_ports) / 2:
                self.current_role = 'leader'
                logging.info(f"Node {self.node_id} becomes the leader for term {self.current_term}")
                # Once becoming a leader, start sending heartbeats
                self.send_heartbeat_to_all()
                self.reset_election_timeout()

    def handle_heartbeat(self, sender_id, sender_term):
        logging.info(f"Node {self.node_id} received heartbeat from node {sender_id} for term {sender_term}")

        if sender_term >= self.current_term:
            self.current_leader = sender_id
            self.current_term = sender_term
            self.current_role = 'follower'
            self.voted_for = None
            # Reset the election timeout upon receiving a valid heartbeat
            self.reset_election_timeout()

    def send_heartbeat_to_all(self):
        logging.info(f"Node {self.node_id}, now leader, sending heartbeat to all nodes for term {self.current_term}")

        message = f"Heartbeat#{self.node_id}#{self.current_term}"
        for port in self.neighbors_ports:
            if port != self.port:  # Don't send to self
                self.socket.sendto(message.encode(), ("127.0.0.1", port))

    def start(self):
        logging.info("Start Raft algorithm...")
        logging.info(f"Node {self.node_id} starting with role {self.current_role}")
        thread = threading.Thread(target=self.listen_messages, name=f"Node{self.node_id}-Listener")
        thread.start()
        logging.info("Listening for incoming messages...")

        # Keep the program running
        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Stopping node...")
            self.stop_event.set()
            thread.join()
            self.election_timeout.cancel()
            self.socket.close()


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.INFO)


def main(heartbeat_duration=1, lb_fault_duration=1, port=1000,
         node_id=1, neighbors_ports=(1000,), is_continue=False):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"lower_bound_fault_duration: {lb_fault_duration}")
        logging.debug(f"upper_bound_fault_duration = {lb_fault_duration}s + 4s")
        logging.debug(f"port: {port}")
        logging.debug(f"neighbors_ports: {neighbors_ports}")
        logging.debug(f"is_continue: {is_continue}")

        logging.info("Create raft object...")
        raft = Raft(node_id, port, neighbors_ports, lb_fault_duration, is_continue, heartbeat_duration)

        logging.info("Execute raft.start()...")
        raft.start()
    except Exception:
        logging.exception("Caught Error")
        raise
