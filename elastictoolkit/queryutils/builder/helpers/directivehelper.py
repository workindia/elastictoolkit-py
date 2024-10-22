class DirectiveHelper:
    """Helper class for directives"""

    @staticmethod
    def normalize_str(param: str):
        def normalize_func(match_params):
            return str.lower(match_params[param])

        return normalize_func
