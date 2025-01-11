# orchestrator/services/queue_service.py

import boto3
import json
import logging
from typing import Optional, Dict, Any
from ..config.settings import settings
from ..utils.s3 import S3Utils

logger = logging.getLogger(__name__)

class QueueService:
    def __init__(self):
        self.sqs = boto3.client('sqs', region_name=settings.REGION_NAME)
        self.s3_utils = S3Utils()
        
        # Queue URLs from settings
        self.task_queue_url = settings.TASK_QUEUE_URL
        self.status_update_queue_url = settings.STATUS_UPDATE_QUEUE_URL
        self.s3_event_queue_url = settings.S3_EVENT_QUEUE_URL

    def send_task_to_queue(self, task_id: str, object_key: str) -> bool:
        """
        Send task details to the SQS Task Queue.
        
        Args:
            task_id (str): The unique task identifier
            object_key (str): The S3 object key
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Generate presigned URLs for the task
            get_url, put_url = self.s3_utils.generate_presigned_urls(object_key)
            
            # Prepare the message
            message_body = {
                "task_id": str(task_id),
                "object_key": object_key,
                "presigned_get_url": get_url,
                "presigned_put_url": put_url
            }

            # Send to SQS
            response = self.sqs.send_message(
                QueueUrl=self.task_queue_url,
                MessageBody=json.dumps(message_body)
            )
            
            logger.info(f"Successfully queued task {task_id}, MessageId: {response['MessageId']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send task to queue: {str(e)}")
            return False

    def poll_status_updates(self) -> Optional[Dict[str, Any]]:
        """
        Poll the SQS Status Update Queue for status updates.
        
        Returns:
            Optional[Dict[str, Any]]: Status update message if available, None otherwise
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.status_update_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        # Parse the message body
                        body = json.loads(message['Body'])
                        
                        # Delete message from queue
                        self.sqs.delete_message(
                            QueueUrl=self.status_update_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        logger.info(f"Received status update for task {body.get('task_id')}")
                        return body
                        
                    except json.JSONDecodeError:
                        logger.error("Failed to parse status update message")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing status update message: {e}")
                        continue
            
            return None

        except Exception as e:
            logger.error(f"Error polling status update queue: {e}")
            return None

    def poll_s3_events(self) -> Optional[Dict[str, Any]]:
        """
        Poll for S3 upload events from the S3 events queue.
        
        Returns:
            Optional[Dict[str, Any]]: S3 event if available, None otherwise
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.s3_event_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        # Parse the event
                        event = json.loads(message['Body'])
                        
                        # Delete message from queue
                        self.sqs.delete_message(
                            QueueUrl=self.s3_event_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        logger.info("Received S3 event")
                        return event
                        
                    except json.JSONDecodeError:
                        logger.error("Failed to parse S3 event message")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing S3 event message: {e}")
                        continue
            
            return None

        except Exception as e:
            logger.error(f"Error polling S3 events queue: {e}")
            return None

    def purge_queues(self) -> bool:
        """
        Purge all messages from the queues. Used for maintenance and testing.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Purge task queue
            self.sqs.purge_queue(QueueUrl=self.task_queue_url)
            
            # Purge status update queue
            self.sqs.purge_queue(QueueUrl=self.status_update_queue_url)
            
            logger.info("Successfully purged all queues")
            return True
            
        except Exception as e:
            logger.error(f"Error purging queues: {e}")
            return False

    def send_status_update(self, task_id: str, status: str, failure_reason: Optional[str] = None) -> bool:
        """
        Send a status update to the status update queue.
        
        Args:
            task_id (str): The task identifier
            status (str): The new status
            failure_reason (Optional[str]): Reason for failure if applicable
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            message_body = {
                "task_id": str(task_id),
                "status": status
            }
            
            if failure_reason:
                message_body["failure_reason"] = failure_reason

            response = self.sqs.send_message(
                QueueUrl=self.status_update_queue_url,
                MessageBody=json.dumps(message_body)
            )
            
            logger.info(f"Sent status update for task {task_id}: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending status update: {e}")
            return False

    def get_queue_attributes(self) -> Dict[str, Any]:
        """
        Get attributes for all queues.
        
        Returns:
            Dict[str, Any]: Queue attributes
        """
        try:
            attributes = {}
            
            # Get task queue attributes
            task_queue_attrs = self.sqs.get_queue_attributes(
                QueueUrl=self.task_queue_url,
                AttributeNames=['All']
            )
            attributes['task_queue'] = task_queue_attrs.get('Attributes', {})
            
            # Get status update queue attributes
            status_queue_attrs = self.sqs.get_queue_attributes(
                QueueUrl=self.status_update_queue_url,
                AttributeNames=['All']
            )
            attributes['status_queue'] = status_queue_attrs.get('Attributes', {})
            
            return attributes
            
        except Exception as e:
            logger.error(f"Error getting queue attributes: {e}")
            return {}

    def handle_dlq_messages(self) -> None:
        """Process messages in Dead Letter Queues if configured."""
        try:
            # Get DLQ URLs from queue attributes
            attributes = self.get_queue_attributes()
            
            # Process DLQ for task queue
            task_dlq = attributes.get('task_queue', {}).get('RedrivePolicy')
            if task_dlq:
                self._process_dlq_messages(json.loads(task_dlq)['deadLetterTargetArn'])
            
            # Process DLQ for status queue
            status_dlq = attributes.get('status_queue', {}).get('RedrivePolicy')
            if status_dlq:
                self._process_dlq_messages(json.loads(status_dlq)['deadLetterTargetArn'])
                
        except Exception as e:
            logger.error(f"Error handling DLQ messages: {e}")

    def _process_dlq_messages(self, dlq_arn: str) -> None:
        """
        Process messages from a specific DLQ.
        
        Args:
            dlq_arn (str): The ARN of the Dead Letter Queue
        """
        try:
            # Convert ARN to URL
            dlq_url = self.sqs.get_queue_url(
                QueueName=dlq_arn.split(':')[-1]
            )['QueueUrl']
            
            while True:
                response = self.sqs.receive_message(
                    QueueUrl=dlq_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1
                )
                
                if 'Messages' not in response:
                    break
                    
                for message in response['Messages']:
                    try:
                        # Log the failed message
                        logger.error(f"Failed message in DLQ: {message['Body']}")
                        
                        # Delete from DLQ
                        self.sqs.delete_message(
                            QueueUrl=dlq_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                    except Exception as e:
                        logger.error(f"Error processing DLQ message: {e}")
                        
        except Exception as e:
            logger.error(f"Error processing DLQ {dlq_arn}: {e}")
