#config/settings.py


import boto3
import yaml
import json
import logging
import os
from typing import Dict, Any

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
                secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
                secret_value = secrets_client.get_secret_value(
                    SecretId='/DEV/audioClientServer/Orchestrator/v2'
                )
                secrets = json.loads(secret_value['SecretString'])
                
                # Set secrets as attributes
                self.API_TOKEN = secrets['api_token']
                self.TASK_QUEUE_URL = secrets['task_queue_url']
                self.STATUS_UPDATE_QUEUE_URL = secrets['status_update_queue_url']
                self.DB_HOST = secrets['db_host']
                self.DB_NAME = secrets['db_name']
                self.DB_USER = secrets['db_username']
                self.DB_PASSWORD = secrets['db_password']
                self.INPUT_BUCKET = secrets['input_bucket']
                self.OUTPUT_BUCKET = secrets['output_bucket']

                # Then load YAML for non-secret configuration
                yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'orchestrator_config.yaml')
                with open(yaml_path, 'r') as file:
                    yaml_config = yaml.safe_load(file)
                    
                self._validate_yaml_config(yaml_config)

                # Set YAML config values as attributes (non-secret config)
                self.REGION_NAME = yaml_config['aws']['region']
                self.POLL_INTERVAL = yaml_config['performance']['poll_interval']
                self.PRESIGNED_URL_EXPIRATION = yaml_config['performance']['presigned_url_expiration']
                
                self._initialized = True
                self.logger.info("Configuration loaded successfully")

            except Exception as e:
                self.logger.critical(f"Failed to load configuration: {str(e)}")
                raise SystemExit("Cannot start application without configuration")
    
    @classmethod
    def get_instance(cls) -> 'Settings':
        if cls._instance is None:
            cls._instance = Settings()
        return cls._instance

    def _validate_yaml_config(self, config: Dict[str, Any]) -> None:
        """Validate non-secret configuration fields from YAML"""
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

# Create a global instance for easy importing
settings = Settings.get_instance()
