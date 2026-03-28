"""
LGN.py - Login module for AngelOne SmartAPI
Reads credentials from .env and exposes a LoginManager class.
"""

import os
import pyotp
from dotenv import load_dotenv
from SmartApi.smartConnect import SmartConnect

# Load environment variables from .env file
load_dotenv()


class Login:
    """Handles authentication with the AngelOne SmartAPI."""

    def __init__(self):
        self.apikey    = os.getenv("API_KEY")
        self.client_id = os.getenv("CLIENT_ID")
        self.pin       = os.getenv("PIN")
        self.token     = os.getenv("TOTP_TOKEN")

        if not all([self.apikey, self.client_id, self.pin, self.token]):
            raise EnvironmentError(
                "One or more required environment variables are missing. "
                "Please check your .env file for: APIKEY, CLIENT_ID, PIN, TOKEN"
            )

    def login(self) -> dict:
        """
        Generates TOTP, creates a SmartAPI session, and returns a normalised
        session dict that main.py can consume directly.

        Returns:
            dict:
                On success:  {'status': 'success', 'connection': <SmartConnect>, 'data': {...}}
                On failure:  {'status': 'error',   'message': str, 'errorcode': str}
        """
        totp          = pyotp.TOTP(self.token).now()
        smart_connect = SmartConnect(self.apikey)
        response      = smart_connect.generateSession(self.client_id, self.pin, totp)

        if response.get("status") is True and response.get("message") == "SUCCESS":
            return {
                "status"     : "success",
                "connection" : smart_connect,       # live SmartConnect object
                "data"       : response["data"],    # clientcode, name, tokens, etc.
            }
        else:
            return {
                "status"    : "error",
                "message"   : response.get("message", "Unknown error"),
                "errorcode" : response.get("errorcode", ""),
            }