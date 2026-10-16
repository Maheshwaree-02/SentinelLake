import time
from functools import wraps
from typing import Callable, Any

# Simple in-memory LRU/TTL cache for demo purposes
# In production, this would use Redis (e.g. aioredis)
_cache = {}

def ttl_cache(ttl_seconds: int = 60):
    """
    Decorator for caching endpoint responses to improve serving resilience
    and reduce database/backend load.
    
    Answers recruiter Q: 'How do you handle traffic spikes to the API?'
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Create a cache key from args and kwargs
            # Simple stringification (works for simple types)
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            now = time.time()
            if key in _cache:
                result, timestamp = _cache[key]
                if now - timestamp < ttl_seconds:
                    return result
            
            # Cache miss or expired
            result = func(*args, **kwargs)
            _cache[key] = (result, now)
            return result
        return wrapper
    return decorator

def clear_cache():
    """Clear the cache (useful for tests or admin endpoints)."""
    _cache.clear()
