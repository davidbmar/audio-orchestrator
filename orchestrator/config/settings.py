# orchestrator/config/settings.py

import boto3
import yaml
import json
import logging
import os
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class Settings:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.logger = logging.getLogger(__name__)
            
            try:
                # First load AWS Secrets - these are our primary configuration source
                self._load_aws_secrets()
                
                # Then load YAML for non-secret configuration
                self._load_yaml_config()
                
                self._initialized = True
                self.logger.info("Configuration loaded successfully")

            except Exception as e:
                self.logger.critical(f"Failed to load configuration: {str(e)}")
                raise SystemExit("Cannot start application without configuration")
    
    def _load_aws_secrets(self) -> None:
        """Load sensitive configuration from AWS Secrets Manager."""
        try:
            secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
            secret_value = secrets_client.get_secret_value(
                SecretId='/DEV/audioClientServer/Orchestrator/v2'
            )
            secrets = json.loads(secret_value['SecretString'])
            
            # Set secrets as attributes
            self.API_TOKEN = secrets['api_token']
            self.TASK_QUEUE_URL = secrets['task_queue_url']
            self.STATUS_UPDATE_QUEUE_URL = secrets['status_update_queue_url']
            self.S3_EVENT_QUEUE_URL = secrets['s3_event_queue_url']
            self.DB_HOST = secrets['db_host']
            self.DB_NAME = secrets['db_name']
            self.DB_USER = secrets['db_username']
            self.DB_PASSWORD = secrets['db_password']
            self.INPUT_BUCKET = secrets['input_bucket']
            self.OUTPUT_BUCKET = secrets['output_bucket']
            
            self.logger.info("AWS Secrets loaded successfully")
            
        except Exception as e:
            self.logger.critical(f"Failed to load AWS Secrets: {str(e)}")
            raise

    def _load_yaml_config(self) -> None:
        """Load non-sensitive configuration from YAML file."""
        try:
            # Get the root directory of the project
            project_root = Path(__file__).parent.parent.parent
            yaml_path = project_root / 'orchestrator_config.yaml'
            
            with open(yaml_path, 'r') as file:
                yaml_config = yaml.safe_load(file)
                
            # Validate YAML configuration
            self._validate_yaml_config(yaml_config)

            # Set YAML config values as attributes
            self.REGION_NAME = yaml_config['aws']['region']
            self.POLL_INTERVAL = yaml_config['performance']['poll_interval']
            self.PRESIGNED_URL_EXPIRATION = yaml_config['performance']['presigned_url_expiration']
            
            self.logger.info("YAML configuration loaded successfully")
            
        except Exception as e:
            self.logger.critical(f"Failed to load YAML configuration: {str(e)}")
            raise

    def _validate_yaml_config(self, config: Dict[str, Any]) -> None:
        """
        Validate required YAML configuration fields.
        
        Args:
            config (Dict[str, Any]): The configuration dictionary to validate
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = {
            'aws': ['region'],
            'performance': ['poll_interval', 'presigned_url_expiration']
        }

        for section, fields in required_fields.items():
            if section not in config:
                raise ValueError(f"Missing required section: {section}")
            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")

    @classmethod
    def get_instance(cls) -> 'Settings':
        """
        Get the singleton instance of Settings.
        
        Returns:
            Settings: The settings instance
        """
        if cls._instance is None:
            cls._instance = Settings()
        return cls._instance

    def reload_configuration(self) -> None:
        """
        Reload configuration from all sources.
        Useful for updating configuration without restarting the application.
        """
        try:
            self._load_aws_secrets()
            self._load_yaml_config()
            self.logger.info("Configuration reloaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to reload configuration: {str(e)}")
            raise

    def get_all_config(self) -> Dict[str, Any]:
        """
        Get all non-sensitive configuration values.
        
        Returns:
            Dict[str, Any]: Dictionary of configuration values
        """
        return {
            'region_name': self.REGION_NAME,
            'poll_interval': self.POLL_INTERVAL,
            'presigned_url_expiration': self.PRESIGNED_URL_EXPIRATION,
            'input_bucket': self.INPUT_BUCKET,
            'output_bucket': self.OUTPUT_BUCKET,
            'db_host': self.DB_HOST,
            'db_name': self.DB_NAME
        }

    def validate_all_settings(self) -> bool:
        """
        Validate that all required settings are present and properly formatted.
        
        Returns:
            bool: True if all settings are valid, False otherwise
        """
        try:
            required_attrs = [
                'API_TOKEN', 'TASK_QUEUE_URL', 'STATUS_UPDATE_QUEUE_URL',
                'S3_EVENT_QUEUE_URL', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
                'INPUT_BUCKET', 'OUTPUT_BUCKET', 'REGION_NAME',
                'POLL_INTERVAL', 'PRESIGNED_URL_EXPIRATION'
            ]
            
            for attr in required_attrs:
                if not hasattr(self, attr):
                    self.logger.error(f"Missing required setting: {attr}")
                    return False
                if getattr(self, attr) is None:
                    self.logger.error(f"Setting is None: {attr}")
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating settings: {e}")
            return False

# Create a global instance for easy importing
settings = Settings.get_instance()
