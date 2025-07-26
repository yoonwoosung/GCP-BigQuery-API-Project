import json
import os
import logging
import functions_framework
import google.auth

from datetime import datetime
from flask import Request, Response
from google.cloud import bigquery

def parse_iso_date(value: str) -> datetime.date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.")

@functions_framework.http
def hello_http(request: Request):
    try:
        _, project = google.auth.default()
        dataset = "assigment_data"

        start_str = request.args.get("start_date")
        end_str = request.args.get("end_date")

        if not start_str or not end_str:
            return Response(
                "Response (400):\n" + json.dumps({"error": "Both start_date and end_date are required"}),
                status=400, mimetype="text/plain"
            )

        try:
            start_date = parse_iso_date(start_str)
            end_date = parse_iso_date(end_str)
        except ValueError as ve:
            return Response(
                "Response (400):\n" + json.dumps({"error": str(ve)}),
                status=400, mimetype="text/plain"
            )

        client = bigquery.Client(project=project)

        query = f"""
            SELECT
                COUNT(1) AS total_orders,
                IFNULL(SUM(o.amount), 0) AS total_amount,
                IFNULL(AVG(o.amount), 0) AS average_amount
            FROM `{project}.{dataset}.orders` as o
            WHERE o.order_date BETWEEN @start_date AND @end_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            ]
        )

        result = list(client.query(query, job_config=job_config).result())[0]


        response_string = json.dumps(dict(result), indent=2)
        return Response(response_string, status=200, mimetype="text/plain")

    except Exception as e:
        logging.exception("Unhandled error")
        return Response(
            "Response (500):\n" + json.dumps({"error": str(e)}),
            status=500, mimetype="text/plain"
        )

