import logging
import os
import sys
from enum import StrEnum
from typing import Any, Dict, Optional, Set


class LogFeature(StrEnum):
    POSTGRES = "postgres"
    TASK_PROCESSING = "task-processing"
    FLOW_CONTROL = "flow-control"
    REMOTE_LOGGING = "remote-logging"
    CRON_SCHEDULING = "cron-scheduling"
    PROCESS_MANAGEMENT = "process-management"
    MISC = "misc"
    ALL = "all"
    TIMEOUT = "timeout"
    SYSTEM = "system"
    LISTENER = "listener"
    DURABILITY = "durability"
    WORKFLOW = "workflow"
    PLATFORM = "platform"
    INIT = "init"
    EXECUTOR = "executor"
    DISPATCHER = "dispatcher"
    REGISTRY = "registry"


class Color(StrEnum):
    RED = "31"
    GREEN = "32"
    YELLOW = "33"
    BLUE = "34"
    MAGENTA = "35"
    CYAN = "36"
    WHITE = "37"
    BRIGHT_RED = "91"
    BRIGHT_GREEN = "92"
    BRIGHT_YELLOW = "93"
    BRIGHT_BLUE = "94"
    BRIGHT_MAGENTA = "95"
    BRIGHT_CYAN = "96"
    BRIGHT_WHITE = "97"


def kv_format(obj: Dict[str, Any]) -> str:
    """Format a dictionary as key=value pairs for logging."""
    return ", ".join(f"{key}={format_value(value)}" for key, value in obj.items())


def format_value(value: Any) -> str:
    """Format a single value for logging."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
        return str(value)  # Simple string representation for nested structures
    return str(value)


class HyrexLogFormatter(logging.Formatter):
    """Custom formatter that adds feature tags and color support."""
    
    def __init__(self, supports_color: bool = True, enabled_features: Optional[Set[LogFeature]] = None):
        super().__init__()
        self.supports_color = supports_color and self._detect_color_support()
        self.enabled_features = enabled_features or {LogFeature.ALL}
        
        # Map log levels to colors
        self.level_colors = {
            logging.DEBUG: Color.BRIGHT_BLUE,
            logging.INFO: Color.CYAN,
            logging.WARNING: Color.YELLOW,
            logging.ERROR: Color.RED,
            logging.CRITICAL: Color.BRIGHT_RED,
        }
    
    def _detect_color_support(self) -> bool:
        """Detect if the terminal supports color output."""
        # Check if colors are explicitly disabled
        if os.environ.get("NO_COLOR") or os.environ.get("PYTHON_DISABLE_COLORS"):
            return False
        
        # Check if colors are explicitly enabled
        if os.environ.get("FORCE_COLOR") or os.environ.get("COLORTERM"):
            return True
        
        # Check if running in CI (usually no color)
        if os.environ.get("CI"):
            return False
        
        # Check if stdout is a TTY
        if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
            term = os.environ.get("TERM")
            if term and term != "dumb":
                return True
        
        return False
    
    def _colorize(self, text: str, color: Color) -> str:
        """Apply ANSI color codes to text."""
        if self.supports_color:
            return f"\033[{color}m{text}\033[0m"
        return text
    
    def format(self, record: logging.LogRecord) -> str:
        # Get feature from record (default to MISC)
        feature = getattr(record, "feature", LogFeature.MISC)
        
        # Check if this feature is enabled
        if not (LogFeature.ALL in self.enabled_features or feature in self.enabled_features):
            return ""  # Don't format if feature is disabled
        
        # Get optional context
        context = getattr(record, "context", None)
        
        # Format the base message
        message = record.getMessage()
        
        # Add context if provided
        if context:
            message = f"{message} | {kv_format(context)}"
        
        # Add feature tag (except for system messages)
        if feature != LogFeature.SYSTEM:
            feature_tag = f"[{feature}]".ljust(22)
            message = f"{feature_tag} {message}"
        
        # Add timestamp and level
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)
        
        # Add PID for multi-process scenarios
        pid = f"[PID: {record.process}]"
        
        # Construct final message
        final_message = f"{pid} {timestamp} - {level} - {message}"
        
        # Apply color based on log level
        if self.supports_color and record.levelno in self.level_colors:
            final_message = self._colorize(final_message, self.level_colors[record.levelno])
        
        return final_message


class HyrexLogger:
    """Hyrex logger with feature-based filtering and context support."""
    
    def __init__(
        self, 
        name: str, 
        feature: LogFeature = LogFeature.MISC,
        enabled_features: Optional[Set[LogFeature]] = None,
        level: str = "INFO"
    ):
        self.name = name
        self.feature = feature
        self.enabled_features = enabled_features or {LogFeature.ALL}
        
        # Create underlying Python logger
        self.logger = logging.getLogger(f"hyrex.{name}")
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Add our custom handler
        handler = logging.StreamHandler()
        handler.setFormatter(HyrexLogFormatter(enabled_features=self.enabled_features))
        handler.setLevel(getattr(logging, level.upper()))  # Set handler level too
        self.logger.addHandler(handler)
        
        # Prevent propagation to avoid duplicate logs
        self.logger.propagate = False
        
        # Store persistent context
        self.context: Dict[str, Any] = {}
    
    def bind(self, **kwargs) -> "HyrexLogger":
        """Add persistent context to this logger."""
        self.context.update(kwargs)
        return self
    
    def _is_feature_enabled(self, feature: Optional[LogFeature] = None) -> bool:
        """Check if a feature is enabled for logging."""
        feature = feature or self.feature
        return LogFeature.ALL in self.enabled_features or feature in self.enabled_features
    
    def _log(
        self, 
        level: int, 
        message: str, 
        feature: Optional[LogFeature] = None,
        **kwargs
    ):
        """Internal log method with feature and context support."""
        feature = feature or self.feature
        
        if not self._is_feature_enabled(feature):
            return
        
        # Merge persistent context with call-specific context
        context = {**self.context, **kwargs} if kwargs or self.context else None
        
        # Create log record with extra fields
        extra = {"feature": feature}
        if context:
            extra["context"] = context
        
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, feature: Optional[LogFeature] = None, **kwargs):
        self._log(logging.DEBUG, message, feature, **kwargs)
    
    def info(self, message: str, feature: Optional[LogFeature] = None, **kwargs):
        self._log(logging.INFO, message, feature, **kwargs)
    
    def warning(self, message: str, feature: Optional[LogFeature] = None, **kwargs):
        self._log(logging.WARNING, message, feature, **kwargs)
    
    def error(self, message: str, feature: Optional[LogFeature] = None, **kwargs):
        self._log(logging.ERROR, message, feature, **kwargs)
    
    def critical(self, message: str, feature: Optional[LogFeature] = None, **kwargs):
        self._log(logging.CRITICAL, message, feature, **kwargs)


def get_logger(
    name: str,
    feature: LogFeature = LogFeature.MISC,
    enabled_features: Optional[Set[LogFeature]] = None,
    level: Optional[str] = None
) -> HyrexLogger:
    """Factory function to create a HyrexLogger."""
    # Get enabled features from environment if not provided
    if enabled_features is None:
        env_features = os.environ.get("HYREX_LOG_FEATURES", "all").split(",")
        enabled_features = {LogFeature(f.strip()) for f in env_features}
    
    # Get log level from environment if not provided
    if level is None:
        level = os.environ.get("HYREX_LOG_LEVEL", "INFO")
    
    return HyrexLogger(name, feature, enabled_features, level)


