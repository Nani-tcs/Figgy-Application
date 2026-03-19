import os
from google.cloud import pubsub_v1

def get_pubsub_publisher_client():
    """Returns a Pub/Sub publisher client instance."""
    return pubsub_v1.PublisherClient()

def get_topic_path(project_id, topic_id):
    """Returns the full topic path for a given topic ID."""
    publisher = get_pubsub_publisher_client()
    return publisher.topic_path(project_id, topic_id)
