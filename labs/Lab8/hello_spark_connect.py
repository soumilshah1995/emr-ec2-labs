#!/usr/bin/env python3
"""Resolve EMR cluster by name, start/reuse a Spark Connect session, run hello-world."""

import time
from typing import Optional, Tuple
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession

PROFILE = "dev"
REGION = "us-east-1"

ACTIVE_STATES = ("SUBMITTED", "STARTING", "STARTED", "IDLE", "BUSY")
READY_STATES = ("IDLE", "BUSY", "STARTED")
FAILED_STATES = ("FAILED", "TERMINATED", "TERMINATING")
CLUSTER_ACTIVE_STATES = ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING")


def log(msg: str) -> None:
    print(f"[spark-connect] {msg}", flush=True)


def redact(value: str, keep: int = 12) -> str:
    if not value:
        return "<empty>"
    if len(value) <= keep * 2:
        return value[:4] + "..."
    return f"{value[:keep]}...{value[-keep:]} (len={len(value)})"


def emr_client():
    log(f"Creating EMR client profile={PROFILE!r} region={REGION!r}")
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return session.client("emr")


def fetch_emr_cluster_id(emr, cluster_name: str) -> str:
    """Look up cluster id from cluster name (active clusters only)."""
    log(f"Looking up cluster name={cluster_name!r}")
    paginator = emr.get_paginator("list_clusters")
    matches = []
    for page in paginator.paginate(ClusterStates=list(CLUSTER_ACTIVE_STATES)):
        for c in page.get("Clusters", []):
            if c.get("Name") == cluster_name:
                matches.append(c)

    if not matches:
        raise ValueError(
            f"No active EMR cluster named {cluster_name!r} "
            f"(states: {', '.join(CLUSTER_ACTIVE_STATES)})"
        )

    preferred = [c for c in matches if c["Status"]["State"] in ("WAITING", "RUNNING")]
    chosen = preferred[0] if preferred else matches[0]
    cluster_id = chosen["Id"]
    log(
        f"OK cluster_name={cluster_name!r} "
        f"cluster_id={cluster_id} state={chosen['Status']['State']}"
    )
    return cluster_id


def find_session_by_name(emr, cluster_id: str, session_name: str) -> Optional[dict]:
    log(f"Checking for existing session name={session_name!r} on {cluster_id}")
    paginator = emr.get_paginator("list_sessions")
    for page in paginator.paginate(
        ClusterId=cluster_id,
        SessionStates=list(ACTIVE_STATES),
    ):
        for s in page.get("Sessions", []):
            if s.get("Name") == session_name:
                log(
                    f"Found existing session id={s['Id']} "
                    f"state={s.get('State')} name={s.get('Name')!r}"
                )
                return s
    log(f"No active session named {session_name!r}")
    return None


def ensure_session(emr, cluster_id: str, session_name: str) -> str:
    existing = find_session_by_name(emr, cluster_id, session_name)
    if existing:
        session_id = existing["Id"]
        log(f"Reusing session_id={session_id} (not creating a new one)")
        return session_id

    log(f"Creating new session name={session_name!r} cluster_id={cluster_id}")
    resp = emr.start_session(ClusterId=cluster_id, Name=session_name)
    session_id = resp["Id"]
    log(
        f"OK start_session session_id={session_id} "
        f"state={resp.get('State')} arn={resp.get('Arn')}"
    )
    return session_id


def wait_until_ready(emr, cluster_id: str, session_id: str, timeout_s: int = 600) -> None:
    log(f"Waiting for session {session_id} to become ready (timeout={timeout_s}s)")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = emr.get_session(ClusterId=cluster_id, SessionId=session_id)
        sess = resp["Session"]
        state = sess["State"]
        log(f"get_session state={state}")
        if state in READY_STATES:
            log(f"OK session ready state={state}")
            return
        if state in FAILED_STATES:
            reason = sess.get("StateChangeReason", "")
            raise RuntimeError(f"Session entered {state}: {reason}")
        time.sleep(5)
    raise TimeoutError(f"Session {session_id} not ready within {timeout_s}s")


def get_session_endpoint_vars(
    emr, cluster_id: str, session_id: str
) -> Tuple[str, str, str, str, str]:
    """
    Fetch endpoint + token and set connection variables.

    Returns: (endpoint, host, auth_token, session_id, connect_url)
    """
    log(f"Calling get_session_endpoint cluster_id={cluster_id} session_id={session_id}")
    ep = emr.get_session_endpoint(ClusterId=cluster_id, SessionId=session_id)

    endpoint = ep["Endpoint"]
    auth_token = ep["AuthToken"]
    expires = ep.get("AuthTokenExpirationTime")
    host = urlparse(endpoint).netloc or endpoint.removeprefix("https://").removeprefix(
        "http://"
    )
    connect_url = (
        f"sc://{host}:443/;"
        f"use_ssl=true;"
        f"x-aws-proxy-auth={auth_token};"
        f"authorization={session_id}"
    )

    log("--- session endpoint vars ---")
    log(f"endpoint          = {endpoint}")
    log(f"host              = {host}")
    log(f"session_id        = {session_id}")
    log(f"auth_token        = {redact(auth_token)}")
    log(f"auth_token_expiry = {expires}")
    log(f"connect_url       = sc://{host}:443/;use_ssl=true;x-aws-proxy-auth=<redacted>;authorization={session_id}")
    log("-----------------------------")
    return endpoint, host, auth_token, session_id, connect_url


def main(cluster_name: str, session_name: str) -> None:
    log(f"START cluster_name={cluster_name!r} session_name={session_name!r}")
    emr = emr_client()

    cluster_id = fetch_emr_cluster_id(emr, cluster_name)
    session_id = ensure_session(emr, cluster_id, session_name)
    wait_until_ready(emr, cluster_id, session_id)

    endpoint, host, auth_token, session_id, connect_url = get_session_endpoint_vars(
        emr, cluster_id, session_id
    )
    log(
        f"Vars set: endpoint={endpoint!r} host={host!r} "
        f"session_id={session_id!r} auth_token={redact(auth_token)}"
    )

    log("Connecting SparkSession.builder.remote(...)")
    spark = SparkSession.builder.remote(connect_url).getOrCreate()
    log(f"OK Spark connected spark.version={spark.version}")

    log("Running hello-world SQL")
    spark.sql("SELECT 'Hello from EMR on EC2' AS message").show()
    log("DONE hello-world succeeded")


if __name__ == "__main__":
    main(cluster_name="spark-connect-cluster", session_name="my-session")
