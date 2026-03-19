import os
import json
import base64
import random
from flask import Flask, request, jsonify
from common.firestore_client import get_firestore_client
from common.pubsub_client import get_pubsub_publisher_client, get_topic_path
from google.cloud import firestore

PROJECT_ID = os.environ.get("GCP_PROJECT")
ORDERS_ACCEPTED_TOPIC_ID = "orders.accepted"
ORDERS_REJECTED_TOPIC_ID = "orders.rejected"

db = get_firestore_client()
publisher = get_pubsub_publisher_client()
orders_accepted_topic_path = get_topic_path(PROJECT_ID, ORDERS_ACCEPTED_TOPIC_ID)
orders_rejected_topic_path = get_topic_path(PROJECT_ID, ORDERS_REJECTED_TOPIC_ID)

app = Flask(__name__)

@app.route("/", methods=["POST"])
def process_order_created():
    envelope = request.get_json()
    if not envelope:
        return 'No Pub/Sub message received', 400
    if not isinstance(envelope, dict) or 'message' not in envelope:
        return 'Invalid Pub/Sub message format', 400

    pubsub_message = envelope['message']

    if 'data' in pubsub_message:
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        payload = json.loads(message_data)
        order_id = payload.get("order_id")

        if not order_id:
            print(f"Invalid order_id in payload: {payload}")
            return 'Invalid order_id', 400

        print(f"Restaurant processing order {order_id}")
        order_ref = db.collection("orders").document(order_id)
        order = order_ref.get()

        if not order.exists:
            print(f"Order {order_id} not found in Firestore. Ignoring.")
            return 'Order not found', 200 # Acknowledge message, idempotent

        current_status = order.to_dict().get("status")
        if current_status != "pending":
            print(f"Order {order_id} already in status '{current_status}'. Skipping restaurant decision.")
            return 'Order already processed by restaurant', 200

        # Simulate restaurant decision (e.g., 80% accept, 20% reject)
        if random.random() < 0.8:
            new_status = "accepted"
            publish_topic_path = orders_accepted_topic_path
            print(f"Restaurant accepted order {order_id}.")
        else:
            new_status = "rejected"
            publish_topic_path = orders_rejected_topic_path
            print(f"Restaurant rejected order {order_id}.")

        # Update Firestore
        order_ref.update({
            "status": new_status,
            "updated_at": firestore.SERVER_TIMESTAMP,
        })

        # Publish to appropriate topic
        try:
            future = publisher.publish(publish_topic_path, json.dumps({"order_id": order_id}).encode("utf-8"))
            future.result()
            print(f"Published order {order_id} status '{new_status}' to Pub/Sub.")
        except Exception as e:
            print(f"Error publishing order {order_id} status '{new_status}': {e}")
            # Consider error handling/dead-letter queue
            return jsonify({"error": f"Failed to publish {new_status} status"}), 500

        return 'Order decision processed', 200

    return 'No data in Pub/Sub message', 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
