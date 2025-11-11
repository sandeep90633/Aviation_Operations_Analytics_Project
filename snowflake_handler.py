import json
import os
from pathlib import Path
import snowflake.connector
from typing import Dict

class SnowflakeHandler:
    def __init__(self,
                 credentials_dir: str,
                 file_name: str):
        """
        Initialize Snowflake manager with connection params

        Args:
            credentials_dir: Path to credentials directory
            file_name: name of the snowflake config file
        """
        self.credentials_dir = Path(credentials_dir)
        self.file_name = file_name
        self.conn = None
        self.sf_options = self._load_config()

    def _load_config(self) -> Dict[str, str]:
        """Load configuration from credential files"""
        env_path = self.credentials_dir / self.file_name

        # Read profiles.yml
        try:
            with open(env_path, 'r') as f:
                target_config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Snowflake config file not found: {env_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in {env_path}")
        
        # Create connection parameters with env var overrides
        return {
            "sfAccount": os.getenv('SNOWFLAKE_ACCOUNT', target_config['account']),
            "sfUser": os.getenv('SNOWFLAKE_USER', target_config['user']),
            "sfPassword": os.getenv('SNOWFLAKE_PASSWORD', target_config['password']),
            "sfDatabase": os.getenv('SNOWFLAKE_DATABASE', target_config['database']),
            "sfSchema": os.getenv('SNOWFLAKE_SCHEMA', target_config['schema']),
            "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE', target_config['warehouse']),
            "sfRole": os.getenv('SNOWFLAKE_ROLE', target_config['role'])
        }

    def connect(self):
        """Establish Snowflake connection"""
        if not self.conn:
            self.conn = snowflake.connector.connect(
                user=self.sf_options['sfUser'],
                password=self.sf_options['sfPassword'],
                account=self.sf_options['sfAccount'],
                warehouse=self.sf_options['sfWarehouse'],
                database=self.sf_options['sfDatabase'],
                schema=self.sf_options['sfSchema'],
                role=self.sf_options['sfRole']
            )
            
            # Simple test query
            with self.conn.cursor() as cur:
                cur.execute("SELECT CURRENT_VERSION()")
                print("Connected to Snowflake:", cur.fetchone()[0])

    def validate_connection(self):
        """Validate that all required parameters are present"""
        conn_params = self.sf_options
        required_params = [
            "sfAccount", "sfUser", "sfPassword",
            "sfDatabase", "sfSchema", "sfWarehouse"
        ]

        missing_params = [
            param for param in required_params
            if not conn_params.get(param)
        ]

        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")

        return True

    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            self.conn = None