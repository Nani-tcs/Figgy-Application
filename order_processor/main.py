import os
import json
import base64
from flask import Flask, request, jsonify
from common.firestore_client import get_firestore_client
from common.pubsub_client import get_pubsub_publisher_client, get_topic_path
from google.cloud import firestore

PROJECT_ID = os.environ.get("GCP_PROJECT")
ORDERS_CREATED_TOPIC_ID = "orders.created"

db = get_firestore_client()
publisher = get_pubsub_publisher_client()
orders_created_topic_path = get_topic_path(PROJECT_ID, ORDERS_CREATED_TOPIC_ID)

app = Flask(__name__)

@app.route("/", methods=["POST"])
def process_order_place():
    envelope = request.get_json()
    if not envelope:
        return 'No Pub/Sub message received', 400
    
    if not isinstance(envelope, dict) or 'message' not in envelope:
        return 'Invalid Pub/Sub message format', 400

    pubsub_message = envelope['message']

    if 'data' in pubsub_message:
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        order_payload = json.loads(message_data)
        
        order_id = order_payload.get("order_id")
        user_id = order_payload.get("user_id")
        restaurant_id = order_payload.get("restaurant_id")
        items = order_payload.get("items")

        if not all([order_id, user_id, restaurant_id, items]):
            print(f"Invalid order payload received: {order_payload}")
            return 'Invalid order payload', 400

        print(f"Processing order {order_id} from {ORDERS_PLACE_TOPIC_ID}")

        order_data = {
            "order_id": order_id,
            "user_id": user_id,
            "restaurant_id": restaurant_id,
            "items": items,
            "status": "pending", # Initial status after processing
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        # 1. Save order to Firestore
        db.collection("orders").document(order_id).set(order_data)
        print(f"Order {order_id} created in Firestore with status 'pending'.")

        # 2. Publish to orders.created topic
        try:
            future = publisher.publish(orders_created_topic_path, json.dumps({"order_id": order_id}).encode("utf-8"))
            future.result()
            print(f"Published order {order_id} to {ORDERS_CREATED_TOPIC_ID}")
        except Exception as e:
            print(f"Error publishing orders.created for {order_id}: {e}")
            # Consider rolling back Firestore or implementing dead-letter queue
            return jsonify({"error": "Failed to publish orders.created"}), 500

        return 'Order processed and published to orders.created', 200

    return 'No data in Pub/Sub message', 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
