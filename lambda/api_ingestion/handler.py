"""
API Ingestion Lambda
Fetches data from REST APIs and stores in S3 raw zone
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import boto3
import requests
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# Configuration
RAW_BUCKET = os.environ.get('RAW_BUCKET', 'data-lake-raw')
API_CONFIG_SECRET = os.environ.get('API_CONFIG_SECRET', 'api-ingestion-config')


def get_api_config() -> Dict[str, Any]:
    """Retrieve API configuration from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=API_CONFIG_SECRET)
        return json.loads(response['SecretString'])
    except ClientError as e:
        logger.error(f"Failed to get API config: {e}")
        raise


def fetch_api_data(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Fetch data from REST API
    
    Args:
        url: API endpoint URL
        headers: Request headers including auth
        params: Query parameters
        
    Returns:
        API response data
    """
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


def upload_to_s3(
    data: Dict[str, Any],
    source_name: str,
    timestamp: datetime
) -> str:
    """
    Upload data to S3 raw zone with partitioning
    
    Args:
        data: Data to upload
        source_name: Name of the data source
        timestamp: Ingestion timestamp
        
    Returns:
        S3 object key
    """
    # Generate partitioned path
    partition_path = (
        f"{source_name}/"
        f"year={timestamp.year}/"
        f"month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/"
        f"hour={timestamp.hour:02d}/"
    )
    
    filename = f"{source_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    object_key = partition_path + filename
    
    # Add metadata
    data_with_metadata = {
        "data": data,
        "metadata": {
            "source": source_name,
            "ingestion_time": timestamp.isoformat(),
            "record_count": len(data) if isinstance(data, list) else 1
        }
    }
    
    try:
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=object_key,
            Body=json.dumps(data_with_metadata, default=str),
            ContentType='application/json',
            Metadata={
                'source': source_name,
                'ingestion_time': timestamp.isoformat()
            }
        )
        logger.info(f"Uploaded to s3://{RAW_BUCKET}/{object_key}")
        return object_key
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise


def process_source(source_config: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
    """
    Process a single data source
    
    Args:
        source_config: Source configuration
        timestamp: Ingestion timestamp
        
    Returns:
        Processing result
    """
    source_name = source_config['name']
    logger.info(f"Processing source: {source_name}")
    
    # Build headers
    headers = {'Content-Type': 'application/json'}
    if source_config.get('api_key'):
        headers['Authorization'] = f"Bearer {source_config['api_key']}"
    
    # Fetch data
    data = fetch_api_data(
        url=source_config['url'],
        headers=headers,
        params=source_config.get('params')
    )
    
    # Upload to S3
    object_key = upload_to_s3(data, source_name, timestamp)
    
    record_count = len(data) if isinstance(data, list) else 1
    
    return {
        'source': source_name,
        'status': 'success',
        'record_count': record_count,
        's3_key': object_key
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler function
    
    Args:
        event: Lambda event (from EventBridge or manual invocation)
        context: Lambda context
        
    Returns:
        Execution results
    """
    logger.info(f"Starting API ingestion. Event: {json.dumps(event)}")
    
    timestamp = datetime.utcnow()
    results = []
    errors = []
    
    try:
        # Get API configurations
        config = get_api_config()
        sources = config.get('sources', [])
        
        # Filter sources if specific one requested
        if event.get('source_name'):
            sources = [s for s in sources if s['name'] == event['source_name']]
        
        # Process each source
        for source_config in sources:
            try:
                result = process_source(source_config, timestamp)
                results.append(result)
            except Exception as e:
                error = {
                    'source': source_config['name'],
                    'status': 'error',
                    'message': str(e)
                }
                errors.append(error)
                logger.error(f"Failed to process {source_config['name']}: {e}")
        
        # Build response
        response = {
            'statusCode': 200 if not errors else 207,  # Multi-status if partial failure
            'body': {
                'timestamp': timestamp.isoformat(),
                'total_sources': len(sources),
                'successful': len(results),
                'failed': len(errors),
                'results': results,
                'errors': errors
            }
        }
        
        logger.info(f"Ingestion complete: {len(results)} success, {len(errors)} failed")
        return response
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'timestamp': timestamp.isoformat()
            }
        }


# For local testing
if __name__ == '__main__':
    test_event = {'source_name': 'test_api'}
    result = handler(test_event, None)
    print(json.dumps(result, indent=2))
