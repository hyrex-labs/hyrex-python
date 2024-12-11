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


def is_glob_pattern(pattern: str) -> bool:
    """Check if pattern contains any unescaped glob special characters."""
    i = 0
    while i < len(pattern):
        # Check for escape character
        if pattern[i] == "\\" and i + 1 < len(pattern):
            i += 2  # Skip both the escape char and the next char
            continue
        # Check for unescaped special characters
        if pattern[i] in "*?{}[]":
            return True
        i += 1
    return False


def glob_to_postgres_regex(glob_pattern: str):
    # Characters that have special meaning in regex and need to be escaped
    # outside of character classes (except the ones we'll handle specially):
    regex_specials = set(".^$+?{}()|\\")

    i = 0
    length = len(glob_pattern)
    result = ["^"]  # Anchor at start

    while i < length:
        c = glob_pattern[i]

        if c == "*":
            # Glob * matches zero or more chars
            result.append(".*")
            i += 1
        elif c == "?":
            # Glob ? matches exactly one char
            result.append(".")
            i += 1
        elif c == "[":
            # Character class start
            i += 1
            result.append("[")
            if i < length and glob_pattern[i] in ("!", "^"):
                # Negation in glob is usually '!' (some shells also support '^')
                # Convert to ^ in regex
                i += 1
                result.append("^")

            # Copy all chars until the closing ']'
            closed = False
            while i < length:
                if glob_pattern[i] == "]":
                    closed = True
                    result.append("]")
                    i += 1
                    break
                else:
                    # In a character class, most chars are literal except backslash and possibly '-'
                    # We'll just pass them through as-is. If you need more robust escaping,
                    # you can handle that here.
                    result.append(glob_pattern[i])
                    i += 1

            if not closed:
                # No closing bracket found, treat as literal '['
                # Append a ']' to close it safely, though this might not match the original intent.
                # Another approach could be to escape the '[' and treat the rest literally.
                result.append("]")
        elif c == "{":
            # Brace expansion {foo,bar,baz} -> (foo|bar|baz)
            i += 1
            brace_content = []
            while i < length and glob_pattern[i] != "}":
                brace_content.append(glob_pattern[i])
                i += 1
            if i < length and glob_pattern[i] == "}":
                # We found a closing brace
                i += 1
                # Split on comma
                parts = "".join(brace_content).split(",")
                result.append("(" + "|".join(parts) + ")")
            else:
                # No closing brace found, treat '{' as literal
                # Escape it for regex
                result.append("\\{")
        else:
            # Normal character
            # If it's a regex special character, escape it
            if c in regex_specials:
                result.append("\\" + c)
            else:
                result.append(c)
            i += 1

    result.append("$")  # Anchor at end
    return "".join(result)
