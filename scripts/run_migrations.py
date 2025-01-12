#!/usr/bin/env python3
# scripts/run_migrations.py

import os
import sys
import boto3
import json
import logging
import psycopg2
from typing import List, Tuple
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_credentials():
    """Get database credentials from AWS Secrets Manager."""
    try:
        # Initialize AWS Secrets Manager client
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        
        # Get the secret
        secret_value = secrets_client.get_secret_value(
            SecretId='/DEV/audioClientServer/Orchestrator/v2'
        )
        secrets = json.loads(secret_value['SecretString'])
        
        # Parse host and port from db_host
        host_port = secrets['db_host'].split(':')
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 5432
        
        return {
            'host': host,
            'port': port,
            'database': secrets['db_name'],
            'user': secrets['db_username'],
            'password': secrets['db_password']
        }
        
    except Exception as e:
        logger.error(f"Failed to get database credentials: {str(e)}")
        raise

def setup_migration_table(conn) -> None:
    """Create migration tracking table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()

def get_applied_migrations(conn) -> List[str]:
    """Get list of already applied migrations."""
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM schema_migrations ORDER BY applied_at")
        return [row[0] for row in cur.fetchall()]

def get_migration_files() -> List[Tuple[str, str]]:
    """Get all migration files from the migrations directory."""
    migrations_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'migrations')
    if not os.path.exists(migrations_dir):
        os.makedirs(migrations_dir)
        
    migration_files = []
    for filename in sorted(os.listdir(migrations_dir)):
        if filename.endswith('.sql'):
            with open(os.path.join(migrations_dir, filename), 'r') as f:
                content = f.read()
            migration_files.append((filename, content))
    
    return migration_files

def apply_migration(conn, filename: str, content: str) -> bool:
    """Apply a single migration."""
    try:
        # Execute migration
        with conn.cursor() as cur:
            logger.info(f"Applying migration: {filename}")
            cur.execute(content)
            
            # Record successful migration
            cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s)",
                (filename,)
            )
            
        conn.commit()
        logger.info(f"Successfully applied migration: {filename}")
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to apply migration {filename}: {str(e)}")
        return False

def verify_connection(conn) -> bool:
    """Verify database connection is working."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()[0] == 1
    except Exception as e:
        logger.error(f"Database connection verification failed: {str(e)}")
        return False

def main():
    """Main migration runner."""
    try:
        # Get database credentials from AWS Secrets Manager
        logger.info("Getting database credentials from AWS Secrets Manager...")
        db_creds = get_db_credentials()
        
        # Log connection details (without password)
        safe_creds = {k:v for k,v in db_creds.items() if k != 'password'}
        logger.info(f"Connecting with credentials: {safe_creds}")
        
        # Connect to database
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**db_creds)
        
        # Verify connection
        if not verify_connection(conn):
            logger.error("Failed to verify database connection")
            sys.exit(1)
        
        logger.info("Database connection verified successfully")
        
        # Setup migration tracking
        setup_migration_table(conn)
        
        # Get applied migrations
        applied = get_applied_migrations(conn)
        logger.info(f"Found {len(applied)} previously applied migrations")
        
        # Get migration files
        migrations = get_migration_files()
        logger.info(f"Found {len(migrations)} migration files")
        
        # Apply new migrations
        success = True
        for filename, content in migrations:
            if filename not in applied:
                if not apply_migration(conn, filename, content):
                    success = False
                    break
        
        if success:
            logger.info("All migrations applied successfully")
        else:
            logger.error("Migration failed - some changes may not have been applied")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Migration error: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
