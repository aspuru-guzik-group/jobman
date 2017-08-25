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


class SchemaMeta(type):
    """Metaclass to propagate doc strings for Schema subclasses."""
    def __new__(mcls, classname, bases, cls_dict):
        cls = super().__new__(mcls, classname, bases, cls_dict)
        for name, member in cls_dict.items():
            if not getattr(member, '__doc__'):
                member.__doc__ = getattr(bases[-1], name).__doc__
        return cls


class Schema(object, metaclass=SchemaMeta):
    """Base class for defining ORM schemas."""

    @classmethod
    def get_field_infos(cls):
        field_infos = {}
        for cls_ in reversed(cls.mro()):
            if cls_ is not cls and hasattr(cls_, 'get_field_infos'):
                field_infos.update(cls_.get_field_infos())
        field_infos.update({
            k: v for k, v in cls.__dict__.items()
            if not k.startswith('_') and k != 'get_field_infos'
        })
        return field_infos


class TimestampedSchemaMixin(Schema):
    """Provides created and modified timestamp fields."""
    created = {'type': 'FLOAT', 'default': generate_timestamp}
    """created"""

    modified = {'type': 'FLOAT', 'auto_update': generate_timestamp}
    """modified"""
