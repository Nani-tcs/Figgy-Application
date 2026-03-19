from google.cloud import firestore

def get_firestore_client():
    """Returns a Firestore client instance."""
    return firestore.Client()
