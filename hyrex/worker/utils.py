import os
import signal


def is_process_alive(pid: int):
    try:
        # Signal 0 is a special "null signal" - it tests existence of the process
        # without sending an actual signal. This is the standard way to check
        # process existence on Unix systems.
        os.kill(pid, 0)
        return True
    except ProcessLookupError:  # No process with this PID exists
        return False
    except PermissionError:  # Process exists but we don't have permission to signal it
        return True


def is_glob_pattern(s: str) -> bool:
    """Check if a string contains any glob special characters."""
    return any(c in s for c in "*?{}[]")


def glob_to_similar(pattern: str) -> str:
    """
    Convert a glob-like pattern into a PostgreSQL SIMILAR TO pattern.

    Conversions:
        *   -> %
        ?   -> _
        {a,b,c} -> (a|b|c)
        [abc] remains [abc]
        [!abc] -> [^abc]

    Literal '%' and '_' are escaped as '\%' and '\_'.
    Other characters remain as-is.

    This allows more direct mapping from common glob patterns to SIMILAR TO patterns.

    Args:
        pattern (str): A glob pattern string.

    Returns:
        str: A SIMILAR TO pattern string.
    """
    if not pattern:
        return pattern

    result = []
    i = 0
    length = len(pattern)

    while i < length:
        char = pattern[i]

        if char == "*":
            # '*' matches zero or more chars, SIMILAR TO uses '%'
            result.append("%")
            i += 1

        elif char == "?":
            # '?' matches exactly one char, SIMILAR TO uses '_'
            result.append("_")
            i += 1

        elif char == "{":
            # Parse brace expansions: {a,b,c} -> (a|b|c)
            brace_count = 1
            start = i + 1
            j = start
            while j < length and brace_count > 0:
                if pattern[j] == "{":
                    brace_count += 1
                elif pattern[j] == "}":
                    brace_count -= 1
                j += 1

            if brace_count == 0:
                # Extract the content inside { }
                inner = pattern[start : j - 1]
                options = inner.split(",")
                # Convert to (a|b|c)
                result.append("(" + "|".join(options) + ")")
                i = j
            else:
                # No matching closing brace, treat '{' literally
                result.append("{")
                i += 1

        elif char == "[":
            # Parse character class or negated character class
            start = i
            j = i + 1
            found_closing = False
            while j < length:
                if pattern[j] == "]":
                    found_closing = True
                    break
                j += 1

            if found_closing:
                # Extract the bracket content, excluding the brackets themselves
                content = pattern[i + 1 : j]

                # Handle negation: [!abc] -> [^abc]
                if content.startswith("!"):
                    content = "^" + content[1:]

                # Append the transformed bracket expression
                result.append("[" + content + "]")
                i = j + 1
            else:
                # No closing bracket, treat '[' literally
                result.append("[")
                i += 1

        elif char in ("%", "_"):
            # Escape literal '%' and '_'
            result.append("\\" + char)
            i += 1

        else:
            # For all other characters, just append as-is.
            result.append(char)
            i += 1

    return "".join(result)
