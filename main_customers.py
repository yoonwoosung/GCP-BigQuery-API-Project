import logging
import google.auth
import functions_framework

from flask import jsonify, Request
from google.cloud import bigquery

@functions_framework.http
def hello_http(request: Request):
    try:
        _, project = google.auth.default()
        dataset = "assigment_data"

        city = request.args.get("city")
        if not city:
            return jsonify({"error": "city query parameter is required"}), 400

        client = bigquery.Client(project=project)

        query = f"""
            SELECT customer_id, name, city, signup_date
            FROM `{project}.{dataset}.customers`
            WHERE city = @city
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city)
            ]
        )

        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())

        response = [
            {
                "customer_id": row.customer_id,
                "name": row.name,
                "city": row.city,
                "signup_date": row.signup_date.isoformat()
            }
            for row in results
        ]

        return jsonify(response)

    except Exception as e:
        logging.exception("Unhandled error")
        return jsonify({"error": str(e)}), 500

