import json
import os
from pathlib import Path
import snowflake.connector
from typing import Dict
import logging
from cryptography.hazmat.primitives import serialization

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
                logging.info("Snowflake configs were loaded.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Snowflake config file not found: {env_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in {env_path}")
        
        # Create connection parameters with env var overrides
        return {
            "sfAccount": os.getenv('SNOWFLAKE_ACCOUNT', target_config['account']),
            "sfUser": os.getenv('SNOWFLAKE_USER', target_config['user']),
            "sfprivate_key_path": os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', target_config['private_key_path']),
            "sfprivate_key_passphrase": os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', target_config['private_key_passphrase']),
            "sfDatabase": os.getenv('SNOWFLAKE_DATABASE', target_config['database']),
            "sfSchema": os.getenv('SNOWFLAKE_SCHEMA', target_config['schema']),
            "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE', target_config['warehouse']),
            "sfRole": os.getenv('SNOWFLAKE_ROLE', target_config['role'])
        }

    def connect(self):
        """Establish Snowflake connection"""
        
        with open(self.sf_options['sfprivate_key_path'], "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=self.sf_options['sfprivate_key_passphrase'].encode()
            )
        if not self.conn:
            self.conn = snowflake.connector.connect(
                user=self.sf_options['sfUser'],
                private_key=p_key,
                account=self.sf_options['sfAccount'],
                warehouse=self.sf_options['sfWarehouse'],
                database=self.sf_options['sfDatabase'],
                schema=self.sf_options['sfSchema'],
                role=self.sf_options['sfRole']
            )
            
            if not self.conn:
                logging.error("Not yet connected.")
                raise NotImplementedError("Not connected but tried to connect.")
            
            # Simple test query
            with self.conn.cursor() as cur:
                cur.execute("SELECT CURRENT_VERSION()")
                logging.info("Connected to Snowflake:", cur.fetchone()[0])

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

        logging.info("All required parameters were given.")
        return True

    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logging.info("Snowflake connection closed.")
            self.conn = None