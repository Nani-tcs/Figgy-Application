import os
import uuid
import json
from flask import Flask, request, jsonify
from common.pubsub_client import get_pubsub_publisher_client, get_topic_path

PROJECT_ID = os.environ.get("GCP_PROJECT")
ORDERS_PLACE_TOPIC_ID = "orders.place"

publisher = get_pubsub_publisher_client()
orders_place_topic_path = get_topic_path(PROJECT_ID, ORDERS_PLACE_TOPIC_ID)

app = Flask(__name__)

@app.route("/orders", methods=["POST"])
def place_order():
    data = request.get_json()
    if not data or not data.get("user_id") or not data.get("restaurant_id") or not data.get("items"):
        return jsonify({"error": "Missing user_id, restaurant_id, or items"}), 400

    order_id = str(uuid.uuid4())
    order_payload = {
        "order_id": order_id,
        "user_id": data["user_id"],
        "restaurant_id": data["restaurant_id"],
        "items": data["items"],
        # Status will be set by Order Processor
    }

    try:
        # Publish order details to orders.place topic
        future = publisher.publish(orders_place_topic_path, json.dumps(order_payload).encode("utf-8"))
        future.result() # Wait for publish to complete
        print(f"Published initial order {order_id} to {ORDERS_PLACE_TOPIC_ID}")
    except Exception as e:
        print(f"Error publishing order {order_id}: {e}")
        return jsonify({"error": "Failed to place order due to publish error"}), 500

    # For status tracking, we'll immediately return the order ID.
    # The actual order will be created in Firestore by the Order Processor.
    return jsonify({"message": "Order initiated successfully", "order_id": order_id}), 202 # Accepted status

@app.route("/orders/<string:order_id>", methods=["GET"])
def get_order_status(order_id):
    from common.firestore_client import get_firestore_client # Import here to avoid circular dependency with app initialization
    db = get_firestore_client()

    order_ref = db.collection("orders").document(order_id)
    order = order_ref.get()

    if not order.exists:
        return jsonify({"error": "Order not found or still processing"}), 404

    return jsonify(order.to_dict()), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
