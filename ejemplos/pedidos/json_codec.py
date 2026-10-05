"""Serializadores JSON explícitos para kafka-python 3.x."""
import json

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, value):
        return json.dumps(value, ensure_ascii=False).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, headers, value):
        if value is None:
            return None
        return json.loads(value.decode("utf-8"))
