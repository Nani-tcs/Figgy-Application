import os
from common.firestore_client import get_firestore_client # Using common client
from google.cloud import firestore

db = get_firestore_client()

def complete_delivery(request):
    """
    HTTP-triggered function invoked by a Cloud Task.
    Updates the order status to 'delivered'.
    """
    data = request.get_json(silent=True)
    if not data or not data.get("order_id"):
        print("No order_id in request payload.")
        return "Bad Request: Missing order_id", 400

    order_id = data["order_id"]
    print(f"Received request to complete delivery for order: {order_id}")

    order_ref = db.collection("orders").document(order_id)
    order = order_ref.get()

    if not order.exists:
        print(f"Order {order_id} not found. Cannot complete delivery.")
        return "Order not found", 404
    
    current_status = order.to_dict().get("status")
    if current_status == "delivered":
        print(f"Order {order_id} already delivered. Skipping update.")
        return "Order already delivered", 200

    try:
        order_ref.update({"status": "delivered", "updated_at": firestore.SERVER_TIMESTAMP})
        print(f"Order {order_id} status updated to 'delivered'.")
        return "OK", 200
    except Exception as e:
        print(f"Error updating order {order_id}: {e}")
        return "Internal Server Error", 500
