#services/queue_service.py

import boto3
import json
import logging
from typing import Optional, Dict, Any
from ..config.settings import settings

logger = logging.getLogger(__name__)

class QueueService:
    def __init__(self):
        self.sqs = boto3.client('sqs', region_name=settings.REGION_NAME)
        self.s3 = boto3.client('s3', region_name=settings.REGION_NAME)

    def send_task_to_queue(self, task_id: str, object_key: str) -> bool:
        """Send task details to the SQS Task Queue."""
        try:
            # Generate pre-signed URLs
            presigned_get_url = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': settings.INPUT_BUCKET, 'Key': object_key},
                ExpiresIn=settings.PRESIGNED_URL_EXPIRATION
            )
            
            presigned_put_url = self.s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': settings.OUTPUT_BUCKET, 
                    'Key': f"transcriptions/{object_key}.txt"
                },
                ExpiresIn=settings.PRESIGNED_URL_EXPIRATION
            )

            message_body = {
                "task_id": str(task_id),
                "object_key": object_key,
                "presigned_get_url": presigned_get_url,
                "presigned_put_url": presigned_put_url
            }

            response = self.sqs.send_message(
                QueueUrl=settings.TASK_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            
            logger.info(f"Successfully queued task {task_id}, MessageId: {response['MessageId']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send task to queue: {str(e)}")
            return False

    def poll_status_updates(self) -> Optional[Dict[str, Any]]:
        """Poll the SQS Status Update Queue for status updates."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=settings.STATUS_UPDATE_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    body = json.loads(message['Body'])
                    # Delete message from queue
                    self.sqs.delete_message(
                        QueueUrl=settings.STATUS_UPDATE_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return body
            
            return None

        except Exception as e:
            logger.error(f"Error polling status update queue: {e}")
            return None

    def poll_s3_events(self) -> Optional[Dict[str, Any]]:
        """Poll for S3 upload events."""
        try:
            response = self.sqs.receive_message(
                QueueUrl="https://sqs.us-east-2.amazonaws.com/635071011057/2024-09-23-audiotranscribe-my-application-queue",
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    event = json.loads(message['Body'])
                    
                    # Delete message from queue
                    self.sqs.delete_message(
                        QueueUrl="https://sqs.us-east-2.amazonaws.com/635071011057/2024-09-23-audiotranscribe-my-application-queue",
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    
                    return event

            return None

        except Exception as e:
            logger.error(f"Error polling S3 events queue: {e}")
            return None
