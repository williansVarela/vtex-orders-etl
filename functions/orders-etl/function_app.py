import logging
import os
from datetime import datetime, timezone

import azure.functions as func
from etl import main
from utils.db import db_connection

app = func.FunctionApp()


@app.schedule(
    schedule="0 */10 * * * *",
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=True,
)
def vtex_orders_integration(myTimer: func.TimerRequest) -> None:
    logging.info(
        "Python timer trigger function ran at %s",
        datetime.now(timezone.utc).isoformat(),
    )
    if myTimer.past_due:
        logging.info("The timer is past due!")

    server = os.getenv("AZURE_SQL_SERVER", "SERVER_NOT_SET")
    port = os.getenv("AZURE_SQL_PORT", "PORT_NOT_SET")
    database = os.getenv("AZURE_SQL_DATABASE", "DATABASE_NOT_SET")
    user = os.getenv("AZURE_SQL_USER", "USER_NOT_SET")
    password = os.getenv("AZURE_SQL_PASSWORD", "PASSWORD_NOT_SET")

    conn = db_connection(server, port, database, user, password)

    try:
        main(conn=conn)
    except Exception as e:
        logging.error(f"Failed to fetch orders from VTEX. Error: {e}")
    finally:
        conn.close()

    logging.info("Function execution completed.")
