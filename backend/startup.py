#!/usr/bin/env python3

import uvicorn
from app.database.connection import init_connection_pool, close_connection_pool
import atexit
import os
import sys
import traceback


def startup():
    """Initialize database connections and start the server"""
    try:
        print("🚀 Starting Brickhouse Brands API...")
        print(f"📁 Environment: DATABRICKS_CLIENT_ID={'set' if os.getenv('DATABRICKS_CLIENT_ID') else 'not set'}")
        print(f"📁 Environment: DB_HOST={os.getenv('DB_HOST', 'not set')}")
        print(f"📁 Environment: DB_USER={os.getenv('DB_USER', 'not set')}")
        print(f"📁 Environment: DB_PASSWORD={'set' if os.getenv('DB_PASSWORD') else 'not set'}")
        print("📁 Initializing database connection pool...")

        # Initialize database connection pool
        try:
            init_connection_pool()
            print("✅ Database connection pool initialized successfully")
        except Exception as e:
            print(f"⚠️ Database connection pool initialization failed: {e}")
            print(f"⚠️ Traceback: {traceback.format_exc()}")
            print("⚠️ Server will start without database connection - API calls will fail")

        # Register cleanup function
        atexit.register(close_connection_pool)

        print("🌐 Starting FastAPI server...")

        # Determine if we should enable reload (only for local dev)
        is_databricks_app = bool(os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"))

        # Start the server
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=not is_databricks_app,  # Disable reload in Databricks Apps
            log_level="info",
        )

    except KeyboardInterrupt:
        print("\n🛑 Server shutdown requested by user")
        close_connection_pool()
        sys.exit(0)
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        print(f"❌ Traceback: {traceback.format_exc()}")
        close_connection_pool()
        sys.exit(1)


if __name__ == "__main__":
    startup()
