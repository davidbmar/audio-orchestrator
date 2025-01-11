Project Structure
```
audio-orchestrator/
├── README.md
├── requirements.txt
├── orchestrator_config.yaml
├── orchestrator/
│   ├── __init__.py
│   ├── main.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── models.py
│   │   └── operations.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── queue_service.py
│   │   └── task_service.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   └── utils/
│       ├── __init__.py
│       └── s3.py

```


# Audio Orchestrator

A service that orchestrates audio file processing tasks, managing the workflow between S3 uploads and worker processes.

## Setup

1. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Configure AWS credentials:
- Ensure you have AWS credentials configured in `~/.aws/credentials` or via environment variables
- The service needs access to:
  - SQS for task queues
  - S3 for file storage
  - Secrets Manager for configuration

3. Create required AWS resources:
- SQS queues for tasks and status updates
- S3 buckets for input and output
- Secrets Manager entry with required configuration

4. Run the service:
```bash
python -m orchestrator.main
```

## Architecture

The service is organized into several components:

- **API**: REST endpoints for worker interaction
- **Database**: Task storage and management
- **Services**: Core business logic
- **Config**: Configuration management
- **Utils**: Helper functions

## Configuration

Configuration is managed through two sources:
1. AWS Secrets Manager for sensitive information
2. YAML file for non-sensitive settings
