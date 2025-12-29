"""
MinIO Storage Service Client

Provides S3-compatible object storage access.
Service endpoint: https://minio.api.whaleforce.dev
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List, Optional, BinaryIO
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

DEFAULT_MINIO_ENDPOINT = "https://minio.api.whaleforce.dev"
DEFAULT_BUCKET = "earnings-analysis"


class MinIOClient:
    """Client for MinIO Storage Service (S3-compatible)."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT)
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "")
        self.bucket = bucket or os.getenv("MINIO_BUCKET", DEFAULT_BUCKET)
        self.timeout = timeout
        self._s3_client = None

    def _get_s3_client(self):
        """Get boto3 S3 client configured for MinIO."""
        if self._s3_client is None:
            try:
                import boto3
                from botocore.config import Config

                self._s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.endpoint,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    config=Config(signature_version="s3v4"),
                )
            except ImportError:
                raise ImportError(
                    "boto3 is required for MinIO client. Install with: pip install boto3"
                )
        return self._s3_client

    def upload_file(
        self,
        file_path: str,
        object_key: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Upload a file to MinIO.

        Args:
            file_path: Local file path to upload
            object_key: S3 object key (defaults to filename)
            metadata: Optional metadata to attach

        Returns:
            Upload result with object key and ETag
        """
        import os.path

        s3 = self._get_s3_client()
        if object_key is None:
            object_key = os.path.basename(file_path)

        extra_args = {}
        if metadata:
            extra_args["Metadata"] = metadata

        s3.upload_file(file_path, self.bucket, object_key, ExtraArgs=extra_args or None)

        return {
            "bucket": self.bucket,
            "key": object_key,
            "uploaded_at": datetime.utcnow().isoformat(),
        }

    def upload_json(
        self,
        data: Any,
        object_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Upload JSON data to MinIO.

        Args:
            data: JSON-serializable data
            object_key: S3 object key
            metadata: Optional metadata to attach

        Returns:
            Upload result
        """
        s3 = self._get_s3_client()
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

        extra_args = {"ContentType": "application/json"}
        if metadata:
            extra_args["Metadata"] = metadata

        s3.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=json_bytes,
            **extra_args,
        )

        return {
            "bucket": self.bucket,
            "key": object_key,
            "size_bytes": len(json_bytes),
            "uploaded_at": datetime.utcnow().isoformat(),
        }

    def download_file(
        self,
        object_key: str,
        file_path: str,
    ) -> Dict[str, Any]:
        """
        Download a file from MinIO.

        Args:
            object_key: S3 object key
            file_path: Local file path to save to

        Returns:
            Download result
        """
        s3 = self._get_s3_client()
        s3.download_file(self.bucket, object_key, file_path)

        return {
            "bucket": self.bucket,
            "key": object_key,
            "downloaded_to": file_path,
        }

    def download_json(self, object_key: str) -> Any:
        """
        Download and parse JSON from MinIO.

        Args:
            object_key: S3 object key

        Returns:
            Parsed JSON data
        """
        s3 = self._get_s3_client()
        response = s3.get_object(Bucket=self.bucket, Key=object_key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)

    def list_objects(
        self,
        prefix: Optional[str] = None,
        max_keys: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        List objects in the bucket.

        Args:
            prefix: Optional prefix filter
            max_keys: Maximum number of keys to return

        Returns:
            List of object metadata
        """
        s3 = self._get_s3_client()
        params = {"Bucket": self.bucket, "MaxKeys": max_keys}
        if prefix:
            params["Prefix"] = prefix

        response = s3.list_objects_v2(**params)

        objects = []
        for obj in response.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
                "etag": obj["ETag"],
            })

        return objects

    def delete_object(self, object_key: str) -> Dict[str, Any]:
        """
        Delete an object from MinIO.

        Args:
            object_key: S3 object key

        Returns:
            Deletion result
        """
        s3 = self._get_s3_client()
        s3.delete_object(Bucket=self.bucket, Key=object_key)

        return {
            "bucket": self.bucket,
            "key": object_key,
            "deleted": True,
        }

    def get_presigned_url(
        self,
        object_key: str,
        expiration: int = 3600,
        method: str = "get_object",
    ) -> str:
        """
        Generate a presigned URL for an object.

        Args:
            object_key: S3 object key
            expiration: URL expiration in seconds
            method: S3 method (get_object or put_object)

        Returns:
            Presigned URL string
        """
        s3 = self._get_s3_client()
        url = s3.generate_presigned_url(
            method,
            Params={"Bucket": self.bucket, "Key": object_key},
            ExpiresIn=expiration,
        )
        return url

    # Convenience methods for earnings analysis

    def save_analysis_result(
        self,
        ticker: str,
        year: int,
        quarter: int,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Save an earnings analysis result.

        Args:
            ticker: Stock ticker
            year: Fiscal year
            quarter: Fiscal quarter
            result: Analysis result data

        Returns:
            Upload result
        """
        object_key = f"analysis/{ticker}/{year}/Q{quarter}/result.json"
        metadata = {
            "ticker": ticker,
            "year": str(year),
            "quarter": str(quarter),
            "analyzed_at": datetime.utcnow().isoformat(),
        }
        return self.upload_json(result, object_key, metadata)

    def get_analysis_result(
        self,
        ticker: str,
        year: int,
        quarter: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a saved analysis result.

        Args:
            ticker: Stock ticker
            year: Fiscal year
            quarter: Fiscal quarter

        Returns:
            Analysis result or None if not found
        """
        object_key = f"analysis/{ticker}/{year}/Q{quarter}/result.json"
        try:
            return self.download_json(object_key)
        except Exception as e:
            logger.warning(f"Could not retrieve analysis for {ticker} {year}Q{quarter}: {e}")
            return None

    def list_analysis_results(
        self,
        ticker: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List available analysis results.

        Args:
            ticker: Optional ticker filter

        Returns:
            List of available analysis keys
        """
        prefix = f"analysis/{ticker}/" if ticker else "analysis/"
        return self.list_objects(prefix=prefix)


# Singleton instance for convenience
_default_client: Optional[MinIOClient] = None


def get_minio_client() -> MinIOClient:
    """Get the default MinIO client instance."""
    global _default_client
    if _default_client is None:
        _default_client = MinIOClient()
    return _default_client
