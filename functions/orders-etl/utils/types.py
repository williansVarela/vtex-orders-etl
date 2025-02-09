from typing import TypedDict


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
