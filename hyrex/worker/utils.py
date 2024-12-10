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

    pattern = "".join(result)
    return f"({pattern})"


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
