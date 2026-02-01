"""
message.py - Message format and helpers.

Defines the standard message structure for the system.
Format: JSON-based with type, sender_id, payload.
"""

import json
import time

# Message Types
TYPE_DISCOVERY = 'DISCOVERY'
TYPE_HEARTBEAT = 'HEARTBEAT'
TYPE_ELECTION = 'ELECTION'
TYPE_COORDINATOR = 'COORDINATOR'
TYPE_PHASE_UPDATE = 'PHASE_UPDATE'
TYPE_ACK = 'ACK'
TYPE_NACK = 'NACK'  # Request missing sequence numbers (omission fault recovery)
TYPE_NACK_RESPONSE = 'NACK_RESPONSE'  # Resend requested messages

def create_message(msg_type, sender_id, payload=None):
    """
    Create a standard message dictionary.
    
    Args:
        msg_type (str): Type of the message (e.g., 'DISCOVERY', 'HEARTBEAT')
        sender_id (str): Unique identifier of the sender
        payload (dict, optional): Additional data specific to the message type
        
    Returns:
        dict: The formatted message
    """
    if payload is None:
        payload = {}
        
    return {
        'type': msg_type,
        'sender_id': sender_id,
        'timestamp': time.time(),
        'payload': payload
    }

def serialize(message):
    """Convert message dict to JSON bytes."""
    return json.dumps(message).encode('utf-8')

def deserialize(data):
    """Convert JSON bytes to message dict."""
    try:
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError:
        return None
