import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import pyodbc
from vtex_client import Order, OrderItem, VtexOrdersAPI

app = func.FunctionApp()

account_name: str = os.getenv("VTEX_ACCOUNT_NAME", "ACCOUNT_NOT_SET")
environment: str = os.getenv("VTEX_ENVIRONMENT", "ENVIRONMENT_NOT_SET")
app_key: str = os.getenv("VTEX_APP_KEY", "APP_KEY_NOT_SET")
app_token: str = os.getenv("VTEX_APP_TOKEN", "APP_TOKEN_NOT_SET")


def get_last_sync_time(cursor: pyodbc.Cursor) -> str:
    query = """
        SELECT TOP 1 CAST(creationDate AS DATETIME)
        FROM VTEX_Orders
        ORDER BY [creationDate] DESC
    """
    result: tuple = cursor.execute(query).fetchone()

    if result:
        last_sync_time = result[0].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return last_sync_time
    return ""


def get_orders(start_date: str, end_date: str) -> tuple[list[Order], list[OrderItem]]:
    item_columns = (
        "id",
        "uniqueId",
        "productId",
        "orderId",
        "ean",
        "quantity",
        "seller",
        "name",
        "refId",
        "price",
        "sellingPrice",
        "sellerSku",
        "measurementUnit",
        "isGift",
    )

    order_columns = (
        "hostname",
        "origin",
        "orderId",
        "creationDate",
        "clientName",
        "status",
        "paymentNames",
        "totalItems",
        "totalValue",
        "statusDescription",
        "salesChannel",
        "orderIsComplete",
        "lastChange",
    )

    client = VtexOrdersAPI(
        account_name=account_name,
        environment=environment,
        app_key=app_key,
        app_token=app_token,
    )

    creation_date = f"creationDate:[{start_date} TO {end_date}]"
    orders_resp = client.get_orders(creation_date=creation_date)

    if orders_resp is None:
        logging.error("Failed to fetch orders")
        return [], []

    orders_summary = orders_resp["list"]

    current_page = orders_resp["paging"]["currentPage"]
    total_pages = orders_resp["paging"]["pages"]

    while current_page < total_pages:
        current_page += 1
        next_orders_resp = client.get_orders(
            creation_date=creation_date, page=current_page
        )

        if next_orders_resp is None:
            continue

        orders_summary.extend(next_orders_resp.get("list", []))

    orders: list[Order] = []
    order_items: list[OrderItem] = []

    for order in orders_summary:
        order = {k: v for k, v in order.items() if k in order_columns}

        order_details = client.get_order(order["orderId"])
        if order_details is None:
            logging.error(
                f"Failed to fetch order details for order: {order['orderId']} - ignoring..."
            )
            continue

        costs = {item["id"]: item["value"] for item in order_details["totals"]}
        order["itemValues"] = costs.get("Items", 0) / 100
        order["discountValue"] = costs.get("Discounts", 0) / 100
        order["shippingValue"] = costs.get("Shipping", 0) / 100
        order["coupon"] = (
            order_details["marketingData"].get("coupon")
            if order_details.get("marketingData")
            else None
        )
        order["clientEmail"] = "-".join(
            order_details["clientProfileData"].get("email", "").split("-")[:-1]
        )
        order["clientId"] = order_details["clientProfileData"].get("document")
        order["state"] = order_details["shippingData"]["address"]["state"]
        order["city"] = order_details["shippingData"]["address"]["city"]

        order["totalValue"] = order["totalValue"] / 100

        orders.append(order)

        items = [
            {k: v for k, v in i.items() if k in item_columns}
            for i in order_details["items"]
        ]
        items: list[OrderItem] = [
            {
                **i,
                "orderId": order["orderId"],
                "totalPrice": i["sellingPrice"] * i["quantity"],
            }
            for i in items
        ]

        order_items.extend(items)

    logging.info(
        f"Fetched {len(orders)} orders and {len(order_items)} order items from VTEX - AmoBeleza"
    )
    return orders, order_items


def main(conn) -> None:
    now = datetime.now(timezone.utc)

    cursor = conn.cursor()

    last_sync_time = get_last_sync_time(cursor)

    if not last_sync_time:
        logging.error("No previous sync found. Fetching all orders")
        return

    end_date = now.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info(f"Fetching orders from {last_sync_time} to {end_date}")
    orders, order_items = get_orders(start_date=last_sync_time, end_date=end_date)

    for order in orders:
        logging.info(f"Inserting order: {order['orderId']}")
        try:
            cursor.execute(
                f"""
                INSERT INTO VTEX_Orders ({', '.join(order.keys())})
                VALUES ({'?,' * (len(order) - 1)}?)
                """,
                tuple(order.values()),
            )
        except Exception as e:
            logging.error(f"Failed to insert order: {order['orderId']}. Error: {e}")

    logging.info(f"Inserted {len(orders)} orders into the database")

    for item in order_items:
        logging.info(f"Inserting order item: {item['uniqueId']}")
        try:
            cursor.execute(
                f"""
                INSERT INTO VTEX_OrderItems ({', '.join(item.keys())})
                VALUES ({'?,' * (len(item) - 1)}?)
                """,
                tuple(item.values()),
            )
        except Exception as e:
            logging.error(
                f"Failed to insert order item: {item['uniqueId']}. Error: {e}"
            )

    conn.commit()

    logging.info(f"Inserted {len(order_items)} order items into the database")
