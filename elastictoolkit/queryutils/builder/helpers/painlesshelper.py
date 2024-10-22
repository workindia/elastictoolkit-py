import re


class PainlessHelper:
    @staticmethod
    def load_script(script_path: str):
        """
        Load a script from a file and return it as a string

        Args:
            script_path: The path to the script file
        Returns:
            str: The script as a string
        """
        script_lines = []
        with open(script_path) as file:
            for line in file:
                # Remove comments and empty lines
                line = re.sub(r"\/\/.*", "", line).strip()
                if len(line) > 0:
                    script_lines.append(line)
        script = " ".join(script_lines)
        return script
