import argparse
import logging
import os
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine


def process_orders(df: pd.DataFrame, engine, table: str):
    orders_translator = {
        "Order": "orderId",
        "Creation Date": "creationDate",
        "Client Name": "clientName",
        "Client Last Name": "clientLastName",
        "Total Value": "totalValue",
        "Payment System Name": "paymentNames",
        "Status": "status",
        "SalesChannel": "salesChannel",
        "Origin": "origin",
        "TotalItems": "totalItems",
        "Host": "hostname",
        "Last Change Date": "lastChange",
        "Discounts Totals": "discountValue",
        "Shipping Value": "shippingValue",
        "Coupon": "coupon",
        "Email": "clientEmail",
        "Client Document": "clientId",
        "UF": "state",
        "City": "city",
    }

    df_orders = df[df.columns.intersection(list(orders_translator.keys()))]
    df_orders = df_orders.rename(columns=orders_translator)
    df_orders = df_orders.drop_duplicates(subset="orderId", ignore_index=True)
    df_orders.paymentNames = df_orders.paymentNames.str.slice(0, 64)
    df_orders.state = df_orders.state.str.slice(0, 2)

    df_orders["clientName"] = (
        df_orders["clientName"].astype(str)
        + " "
        + df_orders["clientLastName"].astype(str)
    )
    df_orders.drop(columns=["clientLastName"], inplace=True)

    df_orders["clientId"] = df_orders["clientId"].astype(str).str.replace(".0", "")
    df_orders["statusDescription"] = ""
    df_orders["orderIsComplete"] = 1
    df_orders["itemValues"] = (
        df_orders["totalValue"]
        - df_orders["discountValue"]
        - df_orders["shippingValue"]
    )

    logging.info(f"Inserting {len(df_orders)} orders into the database")

    df_orders.to_sql(table, engine, if_exists="append", index=False)

    logging.info("Order data inserted successfully")


def process_order_items(df: pd.DataFrame, engine, table: str):
    order_items_translator = {
        "Sequence": "id",
        "Order": "orderId",
        "ID_SKU": "productId",
        "Quantity_SKU": "quantity",
        "Seller Name": "seller",
        "SKU Name": "name",
        "Reference Code": "refId",
        "SKU Value": "price",
        "SKU Selling Price": "sellingPrice",
        "SKU Total Price": "totalPrice",
    }

    seller_name = os.getenv("SELLER_NAME", "")
    seller_map = {seller_name: 1}

    df_order_items = df[df.columns.intersection(list(order_items_translator.keys()))]
    df_order_items = df_order_items.rename(columns=order_items_translator)
    df_order_items["uniqueId"] = df_order_items["id"].apply(lambda x: str(uuid4()))
    df_order_items["ean"] = ""
    df_order_items["sellerSku"] = df_order_items["productId"]
    df_order_items["measurementUnit"] = "un"
    df_order_items["isGift"] = 0
    df_order_items.seller = df_order_items.seller.map(seller_map)

    logging.info(f"Inserting {len(df_order_items)} order items into the database")

    df_order_items.to_sql(table, engine, if_exists="append", index=False)

    logging.info("Order items data inserted successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        help="The directory containing the files to process.",
    )
    args = parser.parse_args()

    processed_folder = "PROCESSED"

    db_conn_str = os.getenv("DB_CONN_STRING", "")
    engine = create_engine(db_conn_str)

    orders_table = os.getenv("ORDERS_TABLE", "")
    order_items_table = os.getenv("ORDER_ITEMS_TABLE", "")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.info("Starting...")

    files = os.listdir(args.directory)
    files.sort()

    for filename in files:
        file_path = os.path.join(args.directory, filename)
        logging.info(f"Reading the file {file_path}")

        if filename.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        elif filename.endswith(".csv"):
            df = pd.read_csv(file_path, sep=";")
        else:
            logging.warning(f"File {filename} is not supported")
            continue

        df["TotalItems"] = df.groupby("Order")["Order"].transform("count")

        logging.info("Processing the orders")
        process_orders(df, engine, orders_table)

        logging.info("Processing the order items")
        process_order_items(df, engine, order_items_table)

        logging.info(f"Finished processing the file {file_path}")

        processed_path = os.path.join(args.directory, processed_folder)
        if not os.path.exists(processed_path):
            os.makedirs(processed_path)

        os.rename(file_path, os.path.join(processed_path, filename))

    logging.info("Finished")
