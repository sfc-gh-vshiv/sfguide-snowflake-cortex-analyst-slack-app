import requests
import generate_jwt
from generate_jwt import JWTGenerator

class CortexChat:
    def __init__(self, account, user, rsa_private_key_path, endpoint, support_tickets_model, supply_chain_model):
        self.account = account
        self.user = user
        self.rsa_private_key_path = rsa_private_key_path
        self.endpoint = endpoint
        self.support_tickets_model = support_tickets_model
        self.supply_chain_model = supply_chain_model
        self.jwt = self.generate_jwt()  # Generate the initial JWT

    def generate_jwt(self):
        """Generate a new JWT token."""
        return generate_jwt.JWTGenerator(self.account, self.user, self.rsa_private_key_path).get_token()

    def query_cortex_analyst(self, prompt) -> dict[str, any]:
        """Query Snowflake Cortex API with proper JWT handling."""
        request_headers = {
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.jwt}",
        }
        request_body = {
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "semantic_models": [
                {"semantic_model_file": f"{self.support_tickets_model}"},
                {"semantic_model_file": f"{self.supply_chain_model}"}
            ]
        }

        response = requests.post(url=self.endpoint, headers=request_headers, json=request_body)

        if response.status_code == 401:  # Unauthorized - likely expired JWT
            print("JWT has expired. Regenerating new JWT...")
            self.jwt = self.generate_jwt()  # Refresh JWT
            request_headers["Authorization"] = f"Bearer {self.jwt}"  
            print("New JWT generated. Retrying request...")
            response = requests.post(url=self.endpoint, headers=request_headers, json=request_body)

        request_id = response.headers.get("X-Snowflake-Request-Id")
        if response.status_code == 200:
            return {**response.json(), "request_id": request_id}
        else:
            raise Exception(
                f"Failed request (id: {request_id}) with status {response.status_code}: {response.text}"
            )
