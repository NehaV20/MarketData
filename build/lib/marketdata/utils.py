import json

def json_response(response):
    """Returns JSON response or logs error"""
    try:
        return response.json()
    except json.JSONDecodeError:
        return {"error": "Failed to decode JSON"}
