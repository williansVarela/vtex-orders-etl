import logging
from http import HTTPStatus
from typing import TypedDict
from urllib.parse import urlencode

from requests import Response, Session


class Order(TypedDict):
    orderId: str
    creationDate: str
    clientName: str
    totalValue: float
    paymentNames: str
    status: str
    statusDescription: str
    salesChannel: str
    origin: str
    orderIsComplete: bool
    totalItems: int
    hostname: str
    lastChange: str | None
    itemValues: int
    discountValue: int | None
    shippingValue: int | None
    coupon: str | None
    clientEmail: str
    clientId: str
    state: str
    city: str


class OrderItem(TypedDict):
    id: str
    uniqueId: str
    productId: str
    orderId: str
    ean: str
    quantity: int
    seller: str
    name: str
    refId: str | None
    price: int
    sellerSku: str
    measurementUnit: str
    isGift: bool


class VtexOrdersAPI:
    def __init__(
        self,
        account_name: str,
        environment: str,
        app_key: str,
        app_token: str,
    ):
        self._account_name = account_name
        self._environment = environment
        self._app_key = app_key
        self._app_token = app_token
        self.base_url = f"https://{self._account_name}.{self._environment}.com/api"
        self._headers = self._set_headers()
        self.session: Session = self._set_session()

    def _set_headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "X-VTEX-API-AppKey": self._app_key,
            "X-VTEX-API-AppToken": self._app_token,
        }

    def _set_session(self) -> Session:
        session = Session()
        session.headers.update(self._headers)
        return session

    def get_order(self, order_id: str) -> dict | None:
        url = f"{self.base_url}/oms/pvt/orders/{order_id}"

        response: Response = self.session.get(url, headers=self._headers)
        if response.status_code == HTTPStatus.OK:
            return response.json()

        logging.error(
            f"Failed to fetch order details from API. Status Code: {response.status_code} - Reason: {response.reason} - Error: {response.text}"
        )
        return None

    def get_orders(
        self, creation_date: str, page: int = 1, per_page: int = 100
    ) -> dict | None:
        url = f"{self.base_url}/oms/pvt/orders"
        params = {
            "f_creationDate": creation_date,
            "page": page,
            "per_page": per_page,
        }

        url = f"{url}?{urlencode(params)}"
        response: Response = self.session.get(url, headers=self._headers)
        if response.status_code == HTTPStatus.OK:
            return response.json()

        logging.error(
            f"Failed to fetch orders from API. Status Code: {response.status_code} - Reason: {response.reason} - Error: {response.text}"
        )
        return None
