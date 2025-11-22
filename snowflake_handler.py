import os
from pathlib import Path
import snowflake.connector
from typing import Dict
import logging
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
class SnowflakeHandler:
    def __init__(self):
        """
        Initialize Snowflake manager with connection params
        
        """
        self.conn = None
        self.sf_options = self._load_config()

    def _load_config(self) -> Dict[str, str]:
        # Create connection parameters with env var overrides
        return {
            "sfAccount": os.getenv('SNOWFLAKE_ACCOUNT'),
            "sfUser": os.getenv('SNOWFLAKE_USER'),
            "sfprivate_key": os.getenv('SNOWFLAKE_PRIVATE_KEY'),
            "sfprivate_key_passphrase": os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
            "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
            "sfSchema": os.getenv('STUDENT_SCHEMA'),
            "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "sfRole": os.getenv('SNOWFLAKE_ROLE')
        }

    def connect(self):
        """Establish Snowflake connection"""
        
        logging.info("Connecting with snowflake with private key......")
        
        private_key_str  = self.sf_options["sfprivate_key"]  # This comes directly from ENV
        private_key_passphrase = self.sf_options["sfprivate_key_passphrase"]

        # Convert string to bytes
        private_key_bytes = private_key_str.encode()
        
        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=private_key_passphrase.encode()
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