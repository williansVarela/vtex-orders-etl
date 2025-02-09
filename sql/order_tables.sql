CREATE TABLE [Orders] (
    orderId NVARCHAR(64) PRIMARY KEY,
    creationDate DATETIMEOFFSET,
    clientName NVARCHAR(256),
    totalValue DECIMAL(18, 2),
    paymentNames NVARCHAR(64),
    "status" NVARCHAR(128),
    statusDescription NVARCHAR(256) NULL,
    salesChannel NVARCHAR(8),
    origin NVARCHAR(64),
    orderIsComplete BIT DEFAULT 1,
    totalItems INT,
    hostname NVARCHAR(128),
    lastChange DATETIMEOFFSET,
    itemValues DECIMAL(18, 2) NULL,
    discountValue DECIMAL(18, 2) NULL,
    shippingValue DECIMAL(18, 2) NULL,
    coupon NVARCHAR(64) NULL,
    clientEmail NVARCHAR(512),
    clientId NVARCHAR(256),
    "state" NVARCHAR(2),
    city NVARCHAR(512)
);

CREATE INDEX idx_creationDate ON [VTEX_Orders](creationDate);

CREATE TABLE [OrderItems] (
    id NVARCHAR(64),
    uniqueId NVARCHAR(256) PRIMARY KEY,
    productId NVARCHAR(64),
    orderId NVARCHAR(64),
    ean NVARCHAR(64),
    quantity INT,
    seller NVARCHAR(16),
    "name" NVARCHAR(1024),
    refId NVARCHAR(128),
    price DECIMAL(18, 2),
    sellingPrice DECIMAL(18, 2),
    totalPrice DECIMAL(18, 2),
    sellerSku NVARCHAR(64),
    measurementUnit NVARCHAR(16),
    isGift BIT,
    FOREIGN KEY (orderId) REFERENCES [Orders](orderId)
);