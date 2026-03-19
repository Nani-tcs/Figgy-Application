import os
import json
from flask import jsonify # Only for jsonify, not for app itself
from google.cloud import firestore, tasks_v2
from google.protobuf import timestamp_pb2
import datetime
from common.firestore_client import get_firestore_client # Using common client

PROJECT_ID = os.environ.get("GCP_PROJECT")
LOCATION_ID = os.environ.get("FUNCTION_REGION") # e.g. us-central1
QUEUE_ID = "delivery-simulation-queue"

# This URL will be for the 'delivery_completion_service' Cloud Function
DELIVERY_COMPLETION_URL = os.environ.get("DELIVERY_COMPLETION_URL") 

db = get_firestore_client()
tasks_client = tasks_v2.CloudTasksClient()

def orchestrate_delivery(request):
    """
    HTTP-triggered Cloud Function.
    Expects JSON payload with 'order_id'.
    Simulates assigning delivery and creates a Cloud Task.
    """
    request_json = request.get_json(silent=True)
    if not request_json or not request_json.get("order_id"):
        return jsonify({"error": "Missing order_id in request payload"}), 400

    order_id = request_json["order_id"]
    print(f"Orchestrating delivery for order: {order_id}")

    order_ref = db.collection("orders").document(order_id)
    order = order_ref.get()

    if not order.exists:
        print(f"Order {order_id} not found in Firestore. Cannot orchestrate delivery.")
        return jsonify({"error": "Order not found"}), 404

    current_status = order.to_dict().get("status")
    if current_status != "accepted":
        print(f"Order {order_id} is in status '{current_status}'. Expected 'accepted'. Skipping delivery orchestration.")
        return jsonify({"message": f"Order {order_id} not accepted, skipping delivery orchestration."}), 200


    # 1. Update status to 'out_for_delivery' and assign a simulated driver
    order_ref.update({
        "status": "out_for_delivery",
        "delivery_agent_id": f"agent_{str(abs(hash(order_id))) % 1000}",
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    print(f"Order {order_id} status updated to 'out_for_delivery'.")

    # 2. Create a Cloud Task to run after a delay (e.g., 1 minute)
    if not DELIVERY_COMPLETION_URL:
        print("DELIVERY_COMPLETION_URL environment variable not set. Cannot create Cloud Task.")
        return jsonify({"error": "DELIVERY_COMPLETION_URL not configured"}), 500

    task_parent = tasks_client.queue_path(PROJECT_ID, LOCATION_ID, QUEUE_ID)

    # Construct the task body
    payload = {"order_id": order_id}
    
    # Set the execution time for 1 minute from now
    in_one_minute = datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(in_one_minute)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": DELIVERY_COMPLETION_URL,
            "headers": {"Content-type": "application/json"},
            "body": json.dumps(payload).encode(),
            # The task must be authenticated to invoke the function
            "oidc_token": {
                "service_account_email": os.environ.get("SERVICE_ACCOUNT_EMAIL", f"figgy-service-account@{PROJECT_ID}.iam.gserviceaccount.com")
            },
        },
        "schedule_time": timestamp,
    }

    try:
        response = tasks_client.create_task(parent=task_parent, task=task)
        print(f"Created Cloud Task {response.name} for order {order_id} to trigger completion.")
    except Exception as e:
        print(f"Error creating Cloud Task for order {order_id}: {e}")
        return jsonify({"error": f"Failed to create Cloud Task: {e}"}), 500

    return jsonify({"message": "Delivery orchestration successful", "order_id": order_id}), 200
