"""Validate config/*.yaml. Usage: python scripts/validate_config.py  (needs pydantic + pyyaml)"""
import sys
sys.path.insert(0, "services/common")
from marketpulse_common.config import ConfigError, load_config

try:
    c = load_config("config")
except ConfigError as e:
    sys.exit(f"CONFIG INVALID: {e}")
print(f"OK: {len(c.instruments)} instruments, {len(c.entities)} entity rules, "
      f"{sum(s.enabled for s in c.sources)}/{len(c.sources)} sources enabled")
