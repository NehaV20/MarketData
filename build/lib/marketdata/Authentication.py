import requests
import logging

from .config import AUTH_BASE_URL


class AuthClient:
    def __init__(self, app_key: str, user_id: str):
        self.app_key = app_key
        self.user_id = user_id

        self.auth_base_url = AUTH_BASE_URL.rstrip("/")

        self.access_token = None

    def _app_login(self):
        login_url = f"{self.auth_base_url}/api/app_login"

        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*"
        }

        payload = {
            "appKey": self.app_key,
            "userId": self.user_id
        }

        response = requests.post(login_url, json=payload, headers=headers)

        if response.status_code == 200:
            json_response = response.json()
            if json_response.get("status") == "success":
                self.access_token = json_response["data"]["accessToken"]
                logging.info("App login successful.")
            else:
                raise Exception(
                    f"App login failed: {json_response.get('message', 'Unknown error')}"
                )
        else:
            raise Exception(
                f"App login failed. Status Code: {response.status_code} - {response.text}"
            )

    def get_access_token(self):
        """Return the access token, logging in if necessary."""
        if not self.access_token:
            self._app_login()
        return self.access_token
