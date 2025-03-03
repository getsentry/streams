from sentry_streams.user_functions.function_template import GroupBy


class GroupByWord(GroupBy):

    def get_group_by_key(self, payload: tuple[str, int]) -> str:
        return payload[0]
