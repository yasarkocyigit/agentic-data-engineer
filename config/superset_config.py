import os

# ─── Secret Key ───
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "openclaw-superset-secret-key-2026")

# ─── Metadata Database (SQLite - default, no extra drivers needed) ───
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# ─── Redis Cache ───
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://superset-redis:6379/0",
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": "redis://superset-redis:6379/1",
}

# ─── CORS & Embedding ───
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["http://localhost:3010", "http://localhost:3000"],
}

# Allow iframe embedding
HTTP_HEADERS = {
    "X-Frame-Options": "ALLOWALL",
}
SESSION_COOKIE_SAMESITE = "Lax"
TALISMAN_ENABLED = False  # Disable CSP for iframe embedding

# ─── Feature Flags ───
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "EMBEDDABLE_CHARTS": True,
    "EMBEDDED_SUPERSET": True,
}

# ─── Default settings ───
WTF_CSRF_ENABLED = False  # Disable for embedded access
PREVENT_UNSAFE_DB_CONNECTIONS = False  # Allow Trino connections
