import time
import uuid


def generate_uuid(*args, **kwargs): return str(uuid.uuid4())


def generate_key(*args, **kwargs): return generate_uuid()


def generate_timestamp(*args, **kwargs): return time.time()


def generate_timestamp_fields():
    return {
        'created': {'type': 'FLOAT', 'default': generate_timestamp},
        'modified': {'type': 'FLOAT', 'auto_update': generate_timestamp}
    }
