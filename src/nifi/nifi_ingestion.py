import os
import json
import ssl
import time
import socket
import http.client
import urllib.parse
from pathlib import Path

NIFI_HOST = "localhost"
NIFI_PORT = 8443
WINDOWS_HOST = "172.26.32.1"

GETFILE_ID = "f390bc69-01a1-1000-cf8f-377671c83f9f"
PUTFILE_ID = "f3972059-01a1-1000-bc5c-428dd07d97cf"

SOURCE_FILE = Path("/workspace/Data/raw/Online Retail.xlsx")
TARGET_FILE = Path("/workspace/Data/processed/Online Retail.xlsx")

TIMEOUT_SECONDS = 180
POLL_INTERVAL = 2

# Route localhost:8443 to the Windows host while preserving TLS SNI.
original_getaddrinfo = socket.getaddrinfo


def route_to_windows(host, port, *args, **kwargs):
    if host == NIFI_HOST and port == NIFI_PORT:
        host = WINDOWS_HOST
    return original_getaddrinfo(host, port, *args, **kwargs)


socket.getaddrinfo = route_to_windows
ssl_context = ssl._create_unverified_context()


def api_request(method, path, token=None, body=None, headers=None):
    request_headers = dict(headers or {})

    if token:
        request_headers["Authorization"] = f"Bearer {token}"

    connection = http.client.HTTPSConnection(
        NIFI_HOST,
        NIFI_PORT,
        context=ssl_context,
        timeout=15,
    )

    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    raw = response.read().decode()
    status = response.status
    connection.close()

    if status not in (200, 201):
        raise RuntimeError(f"NiFi API error {status}: {raw[:1000]}")

    return json.loads(raw) if raw else {}


def authenticate():
    form = urllib.parse.urlencode({
        "username": os.environ["NIFI_USERNAME"],
        "password": os.environ["NIFI_PASSWORD"],
    })

    connection = http.client.HTTPSConnection(
        NIFI_HOST,
        NIFI_PORT,
        context=ssl_context,
        timeout=15,
    )
    connection.request(
        "POST",
        "/nifi-api/access/token",
        form,
        {"Content-Type": "application/x-www-form-urlencoded"},
    )

    response = connection.getresponse()
    token = response.read().decode()
    status = response.status
    connection.close()

    if status != 201 or not token:
        raise RuntimeError(f"NiFi authentication failed: HTTP {status}")

    return token


def get_processor(token, processor_id):
    return api_request(
        "GET",
        f"/nifi-api/processors/{processor_id}",
        token=token,
    )


def set_processor_state(token, processor_id, desired_state):
    processor = get_processor(token, processor_id)
    current_state = processor["component"]["state"]

    if current_state == desired_state:
        return

    payload = {
        "revision": processor["revision"],
        "state": desired_state,
        "disconnectedNodeAcknowledged": False,
    }

    api_request(
        "PUT",
        f"/nifi-api/processors/{processor_id}/run-status",
        token=token,
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )

    deadline = time.monotonic() + 30

    while time.monotonic() < deadline:
        current_state = get_processor(
            token, processor_id
        )["component"]["state"]

        if current_state == desired_state:
            return

        time.sleep(1)

    raise TimeoutError(
        f"Processor {processor_id} did not reach {desired_state}"
    )


def main():
    if not SOURCE_FILE.is_file():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    source_size = SOURCE_FILE.stat().st_size

    print("Connecting to NiFi...")
    token = authenticate()
    print("NiFi authentication successful.")

    # Ensure a clean start and a running destination processor.
    set_processor_state(token, GETFILE_ID, "STOPPED")
    set_processor_state(token, PUTFILE_ID, "RUNNING")

    previous_mtime = (
        TARGET_FILE.stat().st_mtime_ns if TARGET_FILE.exists() else None
    )

    print("Starting GetFile...")
    set_processor_state(token, GETFILE_ID, "RUNNING")

    try:
        deadline = time.monotonic() + TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            if TARGET_FILE.is_file():
                current = TARGET_FILE.stat()

                fresh_copy = (
                    previous_mtime is None
                    or current.st_mtime_ns != previous_mtime
                )

                if fresh_copy and current.st_size == source_size:
                    print("NiFi ingestion completed successfully.")
                    print(f"File: {TARGET_FILE.name}")
                    print(f"Size: {current.st_size:,} bytes")
                    return

            time.sleep(POLL_INTERVAL)

        raise TimeoutError(
            f"NiFi did not copy {TARGET_FILE.name} "
            f"within {TIMEOUT_SECONDS} seconds."
        )

    finally:
        print("Stopping GetFile...")
        set_processor_state(token, GETFILE_ID, "STOPPED")
        print("GetFile stopped.")


if __name__ == "__main__":
    try:
        main()
    finally:
        socket.getaddrinfo = original_getaddrinfo
