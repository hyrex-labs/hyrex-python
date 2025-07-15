from hyrex.dispatcher.dispatcher_provider import get_dispatcher


class HyrexKV:
    """A simple key-value store for Hyrex applications.
    
    Provides string-to-string key-value storage with a maximum value size of 1MB.
    Can be used anywhere in your Hyrex tasks or application code.
    
    Example:
        from hyrex import HyrexKV
        
        # Set a value
        HyrexKV.set("user:123", "John Doe")
        
        # Get a value
        name = HyrexKV.get("user:123")  # Returns "John Doe"
        
        # Delete a value
        HyrexKV.delete("user:123")
    """
    
    MAX_VALUE_SIZE = 1_048_576  # 1MB in bytes
    
    @staticmethod
    def set(key: str, value: str) -> None:
        """Store a key-value pair.
        
        Args:
            key: The key to store the value under
            value: The string value to store (max 1MB)
            
        Raises:
            ValueError: If the value exceeds 1MB
            TypeError: If key or value is not a string
        """
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
        if not isinstance(value, str):
            raise TypeError(f"Value must be a string, got {type(value).__name__}")
            
        # Check value size (UTF-8 encoded)
        value_size = len(value.encode('utf-8'))
        if value_size > HyrexKV.MAX_VALUE_SIZE:
            raise ValueError(
                f"Value exceeds maximum size of 1MB. "
                f"Got {value_size:,} bytes"
            )
        
        dispatcher = get_dispatcher()
        dispatcher.kv_set(key, value)
    
    @staticmethod
    def get(key: str, default: str | None = None) -> str | None:
        """Retrieve a value by key.
        
        Args:
            key: The key to look up
            default: Default value to return if key doesn't exist
            
        Returns:
            The stored value, or default if key doesn't exist
            
        Raises:
            TypeError: If key is not a string
        """
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
            
        dispatcher = get_dispatcher()
        value = dispatcher.kv_get(key)
        return value if value is not None else default
    
    @staticmethod
    def delete(key: str) -> None:
        """Delete a key-value pair.
        
        Args:
            key: The key to delete
            
        Raises:
            TypeError: If key is not a string
        """
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
            
        dispatcher = get_dispatcher()
        dispatcher.kv_delete(key)