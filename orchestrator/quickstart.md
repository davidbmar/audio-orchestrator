# Quick Start Guide for Audio Orchestrator

## Prerequisites
- Python 3.10 or higher
- AWS credentials configured
- PostgreSQL database
- Required AWS resources:
  - SQS queues (task, status update, S3 event queues)
  - S3 buckets (input and output)
  - AWS Secrets Manager entry at `/DEV/audioClientServer/Orchestrator/v2`

## First-Time Setup

1. Install Python venv package if not already installed:
```bash
sudo apt update
sudo apt install python3.10-venv
```

2. Create and activate virtual environment:
```bash
cd ~/working/audio-orchestrator  # Navigate to project root
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip3 install -r requirements.txt
```

4. Ensure config file is in the correct location:
```bash
# Verify orchestrator_config.yaml is in project root
# If it's in orchestrator/orchestrator_config.yaml, move it:
mv orchestrator/orchestrator_config.yaml .
```

## Starting the Service

1. Navigate to project root:
```bash
cd ~/working/audio-orchestrator
```

2. Activate virtual environment:
```bash
source venv/bin/activate
```

3. Start the service:
```bash
PYTHONPATH=. python3 -m orchestrator.main
```

## Verifying Service

Once running, you can verify the service is working by:
```bash
curl http://localhost:6000/health
```

## Troubleshooting

Common issues:
1. "No such file" for orchestrator_config.yaml
   - Ensure you're in project root
   - Verify config file is in root directory

2. Import errors
   - Make sure to use PYTHONPATH=. when running
   - Always run as module: python3 -m orchestrator.main

3. AWS errors
   - Check AWS credentials are configured
   - Verify all required AWS resources exist

4. Database connection errors
   - Verify PostgreSQL is running
   - Check database credentials in AWS Secrets Manager

## Stopping the Service

To stop the service:
1. Press Ctrl+C to stop the main process
2. Deactivate virtual environment:
```bash
deactivate
```
