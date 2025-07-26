import json
import os
import logging
import functions_framework
import google.auth

from flask import Response, Request
from google.cloud import bigquery

@functions_framework.http
def hello_http(request: Request):
    try:
        _, project = google.auth.default()
        dataset = "assigment_data"

        order_id_str = request.args.get("order_id")
        if not order_id_str:
            return Response("Response (400):\n{\"error\": \"order_id parameter is required\"}", status=400, mimetype='text/plain')

        try:
            order_id = int(order_id_str)
        except ValueError:
            return Response("Response (400):\n{\"error\": \"order_id must be an integer\"}", status=400, mimetype='text/plain')

        client = bigquery.Client(project=project)

        query1 = f"""
            SELECT o.order_id, o.order_date, c.customer_id, c.name AS customer_name, c.city
            FROM {project}.{dataset}.orders o
            JOIN {project}.{dataset}.customers c
            ON o.customer_id = c.customer_id
            WHERE o.order_id = @order_id
        """

        config1 = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("order_id", "INT64", order_id)]
        )

        result1 = list(client.query(query1, job_config=config1).result())
        if not result1:
            return Response(f"Response (404):\n{{\"error\": \"Order ID {order_id} not found\"}}", status=404, mimetype='text/plain')

        order_info = result1[0]

        query2 = f"""
            SELECT p.product_id, p.product_name, p.category,
                   oi.quantity, oi.unit_price,
                   oi.quantity * oi.unit_price AS total_price
            FROM {project}.{dataset}.order_items oi
            JOIN {project}.{dataset}.products p
            ON oi.product_id = p.product_id
            WHERE oi.order_id = @order_id
            ORDER BY p.product_id ASC
        """

        config2 = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("order_id", "INT64", order_id)]
        )

        items_result = list(client.query(query2, job_config=config2).result())
        items = []
        order_total = 0

        for row in items_result:
            item = {
                "product_id": row.product_id,
                "product_name": row.product_name,
                "category": row.category,
                "quantity": row.quantity,
                "unit_price": float(row.unit_price),
                "total_price": float(row.total_price)
            }
            items.append(item)
            order_total += item["total_price"]

        response_data = {
            "order_id": order_info.order_id,
            "order_date": order_info.order_date.isoformat(),
            "customer": {
                "customer_id": order_info.customer_id,
                "name": order_info.customer_name,
                "city": order_info.city
            },
            "items": items,
            "order_total": order_total
        }

        response_string =  json.dumps(response_data, indent=2)
        return Response(response_string, status=200, mimetype='text/plain')

    except Exception as e:
        logging.exception("Unhandled error")
        return Response("Response (500):\n" + json.dumps({"error": str(e)}), status=500, mimetype='text/plain')
