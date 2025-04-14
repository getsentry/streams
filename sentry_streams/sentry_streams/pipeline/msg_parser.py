from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

# # # Only supports JSON now
# class Serializer(ABC):

#     @abstractmethod
#     def serialize(self, msg: Mapping[str, Any]) -> Any:
#         raise NotImplementedError


# # class JSONSerializer(Serializer):

#     def __init__(self) -> None:
#         pass

#     def serialize(self, msg: Mapping[str, Any]) -> Any:
#         return dumps(msg)


def json_parser(msg: bytes) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def json_serializer(msg: Mapping[str, Any]) -> Any:
    return dumps(msg)


# class ValidatingParser:
#     """
#     Handles both deserialization and schema validation
#     of a message payload
#     """

#     def __init__(self, deserializer: Deserializer):
#         self.de = deserializer

#     def parse(self, msg: bytes) -> Mapping[str, Any]:
#         return self.de.deserialize(msg)
