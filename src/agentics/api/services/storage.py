# src/agentics/api/services/storage.py

import os
import shutil
import boto3
from abc import ABC, abstractmethod
from typing import BinaryIO
from pathlib import Path
from botocore.exceptions import ClientError
from loguru import logger

from agentics.api.config import settings, StorageBackend


class StorageProvider(ABC):
    @abstractmethod
    def upload(self, file_obj: BinaryIO, destination: str) -> str:
        """Uploads file and returns a reference path/key."""
        pass

    @abstractmethod
    def download_path(self, file_reference: str) -> str:
        """Returns a local filesystem path to the file."""
        pass

    @abstractmethod
    def delete(self, file_reference: str):
        pass


class LocalStorageProvider(StorageProvider):
    def __init__(self):
        self.base_path.mkdir(parents=True, exist_ok=True)

    def upload(self, file_obj: BinaryIO, destination: str) -> str:
        full_path = self.base_path / destination
        # Reset file pointer just in case
        file_obj.seek(0)
        with open(full_path, "wb") as buffer:
            shutil.copyfileobj(file_obj, buffer)
        return str(full_path)

    def download_path(self, file_reference: str) -> str:
        # In local mode, the reference is likely the absolute path,
        # but we ensure it exists.
        if not os.path.exists(file_reference):
            # Fallback: check if it's relative to base_path
            relative_check = self.base_path / Path(file_reference).name
            if relative_check.exists():
                return str(relative_check)
            raise FileNotFoundError(f"File {file_reference} not found locally.")
        return file_reference

    def delete(self, file_reference: str):
        if os.path.exists(file_reference):
            try:
                os.remove(file_reference)
            except OSError as e:
                logger.error(f"Error deleting local file {file_reference}: {e}")


class S3StorageProvider(StorageProvider):
    def __init__(self):
        self.bucket_name = settings.AWS_BUCKET_NAME

        # If keys are present in env, use them (Local Dev).
        # If not, let boto3 find the IAM Role (AWS Production).
        boto_config = {"region_name": settings.AWS_REGION}

        if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
            boto_config.update(
                {
                    "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
                }
            )

        self.s3_client = boto3.client("s3", **boto_config)

        # Local cache for processing files after download
        self.local_cache = Path("/tmp/agentics_s3_cache")
        self.local_cache.mkdir(parents=True, exist_ok=True)

    def upload(self, file_obj: BinaryIO, destination: str) -> str:
        file_obj.seek(0)
        try:
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, destination)
            return destination
        except ClientError as e:
            logger.error(f"S3 Upload failed: {e}")
            raise

    def download_path(self, file_reference: str) -> str:
        # We assume file_reference is the S3 Key
        local_path = self.local_cache / file_reference

        # Simple cache check
        if not local_path.exists():
            try:
                # Ensure parent dir exists (if key has slashes)
                local_path.parent.mkdir(parents=True, exist_ok=True)

                self.s3_client.download_file(
                    self.bucket_name, file_reference, str(local_path)
                )
            except ClientError as e:
                logger.error(f"S3 Download failed: {e}")
                raise FileNotFoundError(f"Could not retrieve {file_reference} from S3")

        return str(local_path)

    def delete(self, file_reference: str):
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_reference)
        except ClientError as e:
            logger.error(f"S3 Delete failed: {e}")


def get_storage_provider() -> StorageProvider:
    if settings.STORAGE_BACKEND == StorageBackend.S3:
        if not settings.AWS_BUCKET_NAME:
            raise ValueError("AWS_BUCKET_NAME must be set for S3 storage")
        return S3StorageProvider()
    return LocalStorageProvider()
