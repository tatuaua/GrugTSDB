import socket
import json
import time
import unittest
import logging # Optional: for better debugging if needed

# Optional: Configure logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class ServerIntegrationTests(unittest.TestCase):
    SERVER_ADDRESS = ('localhost', 8080)  # Replace with your server's address and port
    BUFFER_SIZE = 1024
    TIMEOUT = 2

    def setUp(self):
        """Set up the client socket before each test."""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.settimeout(self.TIMEOUT)
        # logging.info(f"Socket created for test: {self.id()}")

    def tearDown(self):
        """Close the client socket after each test."""
        self.client_socket.close()
        # logging.info(f"Socket closed for test: {self.id()}")

    def _send_and_receive(self, message):
        """
        Sends a JSON message to the server.
        Returns the decoded JSON response if the actionType was 'read',
        otherwise returns the raw string response.
        """
        action_type = message.get("actionType") # Get action type for conditional parsing
        # logging.debug(f"Sending message: {message}")

        try:
            serialized_message = json.dumps(message).encode('utf-8')
            self.client_socket.sendto(serialized_message, self.SERVER_ADDRESS)
            # logging.debug(f"Message sent to {self.SERVER_ADDRESS}")

            data, server_addr = self.client_socket.recvfrom(self.BUFFER_SIZE)
            # logging.debug(f"Received {len(data)} bytes from {server_addr}")
            decoded_data = data.decode('utf-8')
            # logging.debug(f"Decoded response: {decoded_data}")

            # Only attempt to parse JSON if the original request was a 'read' action
            if action_type == "read" or action_type == "aggregateRead":
                try:
                    # logging.debug("Attempting JSON decode for 'read' action.")
                    return json.loads(decoded_data)
                except json.JSONDecodeError as json_err:
                    # If a read action doesn't return valid JSON, it's an error
                    self.fail(f"Received invalid JSON for 'read' action: {decoded_data}. Error: {json_err}")
            else:
                # For non-read actions, return the raw string response
                # logging.debug("Returning raw string response for non-'read' action.")
                return decoded_data

        except socket.timeout:
            # logging.error(f"Timeout occurred for message: {message}")
            self.fail(f"Timeout occurred ({self.TIMEOUT}s) while waiting for response to: {message}")
        except json.JSONDecodeError as e:
            # This might happen if the *request* message itself is not valid JSON
            # (Shouldn't happen with this code, but good to be aware)
             # logging.error(f"Error encoding request message: {message}. Error: {e}")
            self.fail(f"Failed to encode the request message as JSON: {message}. Error: {e}")
        except Exception as e:
            # Catch other potential errors (e.g., network issues before recvfrom)
            # logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            self.fail(f"An unexpected error occurred: {e}")

    # --- Test methods updated to handle string vs JSON responses ---

    def test_create_bucket(self):
        """Tests creating a new bucket."""
        bucket_name = f"test_bucket_{int(time.time())}" # Unique name
        message = {
            "actionType": "createBucket",
            "bucketName": bucket_name,
            "fields": [
                {"name": "timestamp", "type": "LONG", "size": 8},
                {"name": "value", "type": "INT", "size": 4}
            ]
        }
        response = self._send_and_receive(message)
        # Expecting a string response for createBucket
        self.assertIsInstance(response, str)
        self.assertIn("Bucket created successfully", response) # Check for success message

    def test_write_to_bucket(self):
        """Tests writing data to an existing bucket."""
        # Create a bucket first (use unique name)
        bucket_name = f"write_test_bucket_{int(time.time())}"
        create_message = {
            "actionType": "createBucket",
            "bucketName": bucket_name,
            "fields": [
                {"name": "timestamp", "type": "LONG", "size": 8},
                {"name": "value", "type": "INT", "size": 4}
            ]
        }
        # Assuming create returns a success string
        create_response = self._send_and_receive(create_message)
        self.assertIn("Bucket created successfully", create_response)


        # Write data to the bucket
        timestamp = 315532800000
        value = 42
        write_message = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": {
                "timestamp": timestamp,
                "value": value
            }
        }
        response = self._send_and_receive(write_message)
        # Expecting a string response for write
        self.assertIsInstance(response, str)
        self.assertIn("Data written to bucket", response) # Check for success message

    def test_read_most_recent(self):
        """Tests reading the most recent record from a bucket."""
        # Create a bucket and write data (use unique name)
        bucket_name = f"read_recent_bucket_{int(time.time())}"
        create_message = {
            "actionType": "createBucket",
            "bucketName": bucket_name,
            "fields": [
                {"name": "timestamp", "type": "LONG", "size": 8},
                {"name": "value", "type": "INT", "size": 4}
            ]
        }
        self._send_and_receive(create_message) # Ignore response

        timestamp = 315532800000
        value = 84
        write_message = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": { "timestamp": timestamp, "value": value }
        }
        self._send_and_receive(write_message) # Ignore response

        # Read the most recent data
        read_message = {
            "actionType": "read",
            "bucketName": bucket_name,
            "type": "MOST_RECENT"
        }
        response = self._send_and_receive(read_message)
        # Expecting JSON (dict) response for read
        self.assertIsInstance(response, dict)
        self.assertEqual(response["data"]["value"], value)
        self.assertEqual(response["data"]["timestamp"], timestamp)

    def test_read_all(self):
        """Tests reading all records from a bucket."""
        # Create a bucket and write multiple records (use unique name)
        bucket_name = f"read_all_bucket_{int(time.time())}"
        create_message = {
            "actionType": "createBucket",
            "bucketName": bucket_name,
            "fields": [
                {"name": "timestamp", "type": "LONG", "size": 8},
                {"name": "value", "type": "INT", "size": 4}
            ]
        }
        self._send_and_receive(create_message) # Ignore response

        ts1 = 315532800000
        val1 = 42
        write_message_1 = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": { "timestamp": ts1, "value": val1 }
        }
        self._send_and_receive(write_message_1) # Ignore response
        time.sleep(0.01) # Small delay to ensure different timestamps
        ts2 = 315532800000
        val2 = 84
        write_message_2 = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": { "timestamp": ts2, "value": val2 }
        }
        self._send_and_receive(write_message_2) # Ignore response

        # Read all data
        read_message = {
            "actionType": "read",
            "bucketName": bucket_name,
            "type": "FULL"
        }
        response = self._send_and_receive(read_message)
        # Expecting JSON (list of dicts) response for read
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 2)
        # Use assertCountEqual for order-independent comparison
        expected_data = [
            {"data": {"timestamp": ts1, "value": 42}},
            {"data": {"timestamp": ts2, "value": 84}}
        ]
        self.assertCountEqual(response, expected_data)

    def test_aggregate_read(self):
        """Tests reading aggregated data from a bucket."""
        # Create a bucket and write multiple records (use unique name)
        bucket_name = f"aggregate_read_bucket_{int(time.time())}"
        create_message = {
            "actionType": "createBucket",
            "bucketName": bucket_name,
            "fields": [
                {"name": "timestamp", "type": "LONG", "size": 8},
                {"name": "value", "type": "INT", "size": 4}
            ]
        }
        self._send_and_receive(create_message)
        # Ignore response
        write_message = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": {
                "timestamp": 315532800000,
                "value": 42
            }
        }
        self._send_and_receive(write_message) # Ignore response
        write_message = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": {
                "timestamp": 315532800001,
                "value": 84
            }
        }
        self._send_and_receive(write_message)
        write_message = {
            "actionType": "write",
            "bucketName": bucket_name,
            "fieldValues": {
                "timestamp": 315532800003,
                "value": 126
            }
        }
        self._send_and_receive(write_message) # Ignore response

        aggregate_message = {
            "actionType": "aggregateRead",
            "bucketName": bucket_name,
            "aggregationType": "sum",
            "fieldName": "value",
            "timeRangeStart": 315532799999,
            "timeRangeEnd": 315532800002
        }
        response = self._send_and_receive(aggregate_message)
        # Expecting JSON (dict) response for aggregateRead
        self.assertIsInstance(response, dict)
        self.assertEqual(response["data"]["value_sum"], 42 + 84)

    def test_create_stream(self):
        """Tests creating a stream."""
        # Note: Assumes the server handles requests for potentially non-existent
        # buckets gracefully, or that 'stream_bucket' is created elsewhere.
        bucket_name = f"stream_bucket_{int(time.time())}"
        # Optionally create the bucket if needed for the stream to be valid
        # create_message = { ... }
        # self._send_and_receive(create_message)

        message = {
            "actionType": "createStream",
            "bucketsToStream": [bucket_name]
        }
        response = self._send_and_receive(message)
        # Expecting a string response for createStream
        self.assertIsInstance(response, str)
        self.assertIn("Stream started for buckets", response) # Check for success message

if __name__ == '__main__':
    unittest.main()