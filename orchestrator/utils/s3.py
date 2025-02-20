# orchestrator/utils/s3.py

import boto3
import logging
from botocore.exceptions import ClientError
from urllib.parse import quote, unquote
import requests
from typing import Optional, Dict, Any, Tuple
from ..config.settings import settings

logger = logging.getLogger(__name__)

class S3Utils:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=settings.REGION_NAME)
        self.input_bucket = settings.INPUT_BUCKET
        self.output_bucket = settings.OUTPUT_BUCKET

    def normalize_s3_key(self, key: str) -> str:
        """
        Normalize a key by fully decoding and then properly encoding it once.
        This handles cases where keys might be double-encoded or partially encoded.
        """
        try:
            # First decode completely (handles multiple encodings)
            decoded = key
            while '%' in decoded:
                prev = decoded
                decoded = unquote(decoded)
                if prev == decoded:  # Stop if no more decoding possible
                    break
                    
            # Now encode once properly, preserving slashes
            encoded = quote(decoded, safe='/')
            
            logger.debug(f"Normalized key from '{key}' to '{encoded}'")
            return encoded
        except Exception as e:
            logger.error(f"Error normalizing key {key}: {e}")
            return key

    def double_decode_key(self, key: str) -> str:
        """
        Handle double URL encoded keys from S3 events.
        Some systems double-encode special characters for safety.
        """
        try:
            # First decode: %257C -> %7C
            once_decoded = unquote(key)
            # Second decode: %7C -> |
            twice_decoded = unquote(once_decoded)
            logger.debug(f"Double decoded key from '{key}' to '{twice_decoded}'")
            return twice_decoded
        except Exception as e:
            logger.error(f"Error decoding key {key}: {e}")
            return key

    def verify_object_exists(self, bucket: str, key: str) -> bool:
        """Verify if an object exists in the specified S3 bucket."""
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking object existence: {e}")
                raise

    def generate_presigned_urls(self, object_key: str, expiration: int = None) -> Tuple[str, str]:
        """
        Generate presigned URLs for getting and putting objects.
        Returns a tuple of (get_url, put_url).
        """
        if expiration is None:
            expiration = settings.PRESIGNED_URL_EXPIRATION

        try:
            # Generate URL for getting the input file
            get_url = self.s3.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.input_bucket,
                    'Key': object_key
                },
                ExpiresIn=expiration
            )

            # Generate URL for putting the output file
            output_key = f"transcriptions/{object_key}.txt"
            put_url = self.s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.output_bucket,
                    'Key': output_key,
                    'ContentType': 'text/plain'
                },
                ExpiresIn=expiration
            )

            return get_url, put_url

        except ClientError as e:
            logger.error(f"Error generating presigned URLs: {e}")
            raise

    # Generated presigned url but this is by task_id used for getting transcriptions by task id.
    def generate_presigned_s3_url(task_id: str) -> Optional[str]:
        try:
            object_key = f"transcriptions/{task_id}.txt"
            return s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': settings.OUTPUT_BUCKET, 'Key': object_key},
                ExpiresIn=3600  # URL valid for 1 hour
            )
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None

    def store_transcription_in_s3(self, task_id: str, transcription: str) -> bool:
        """Uploads the transcription to S3."""
        # You can either use the instance's client and bucket info:
        bucket_name = self.output_bucket
        object_key = f"transcriptions/{task_id}.txt"
        try:
            self.s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=transcription.encode("utf-8"),
                ContentType="text/plain"
            )
            logger.info(f"Successfully stored transcription for task {task_id} in S3.")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload transcription for task {task_id}: {e}")
            return False

    def normalize_and_verify_key(self, key: str) -> Optional[str]:
        """
        Normalize an S3 key and verify it exists in the input bucket.
        Returns the normalized key if successful, None otherwise.
        """
        try:
            # Try different key formats
            key_variants = [
                key,  # Original
                self.normalize_s3_key(key),  # Normalized
                self.double_decode_key(key),  # Double decoded
                quote(unquote(key), safe='/'),  # Decoded and re-encoded
                quote(key, safe='/'),  # Just encoded
            ]

            for variant in key_variants:
                if self.verify_object_exists(self.input_bucket, variant):
                    logger.info(f"Found matching key format: {variant}")
                    return variant

            logger.error(f"Could not find valid key format for: {key}")
            return None

        except Exception as e:
            logger.error(f"Error in normalize_and_verify_key for {key}: {e}")
            return None

    def get_object_metadata(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Get object metadata from S3."""
        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            logger.error(f"Error getting object metadata: {e}")
            return None

    def clean_up_failed_uploads(self, object_key: str) -> None:
        """Clean up any failed or partial uploads for a given key."""
        try:
            # List any multipart uploads
            response = self.s3.list_multipart_uploads(
                Bucket=self.output_bucket,
                Prefix=f"transcriptions/{object_key}"
            )

            # Abort any in-progress multipart uploads
            if 'Uploads' in response:
                for upload in response['Uploads']:
                    self.s3.abort_multipart_upload(
                        Bucket=self.output_bucket,
                        Key=upload['Key'],
                        UploadId=upload['UploadId']
                    )
                    logger.info(f"Aborted multipart upload for key: {upload['Key']}")

        except ClientError as e:
            logger.error(f"Error cleaning up failed uploads: {e}")

    def copy_object(self, source_key: str, dest_key: str, 
                   source_bucket: Optional[str] = None,
                   dest_bucket: Optional[str] = None) -> bool:
        """
        Copy an object within S3. If buckets not specified, uses default input/output buckets.
        Returns True if successful, False otherwise.
        """
        try:
            source_bucket = source_bucket or self.input_bucket
            dest_bucket = dest_bucket or self.output_bucket

            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }

            self.s3.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            logger.info(f"Successfully copied {source_key} to {dest_key}")
            return True

        except ClientError as e:
            logger.error(f"Error copying object: {e}")
            return False

    def list_bucket_objects(self, prefix: str = '', 
                          bucket: Optional[str] = None) -> list:
        """List objects in a bucket with the given prefix."""
        try:
            bucket = bucket or self.input_bucket
            objects = []
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            
            return objects
        except ClientError as e:
            logger.error(f"Error listing bucket objects: {e}")
            return []
