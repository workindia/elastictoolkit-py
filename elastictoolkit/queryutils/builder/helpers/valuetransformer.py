from typing import Callable


class ValueTransformer:
    """Helper class for value transformers : These are functions that will be called during runtime to transform values"""

    @staticmethod
    def unpacked(func: Callable):
        """
        Sets a property `unpack` to True. This signals `ValueParser` to unpack values
        when appending items to list when calling this func.
        """
        func.unpack = True
        return func

    @staticmethod
    def normalize_str(param: str):
        def normalize_func(match_params):
            return str.lower(match_params[param])

        return normalize_func
