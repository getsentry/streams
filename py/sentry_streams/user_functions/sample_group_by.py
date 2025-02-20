def my_group_by(msg_payload: tuple[str, int]) -> str:
    return msg_payload[0]


def dumb_group_by(msg: str) -> str:
    return msg


# lambda x: x[0] simplest
