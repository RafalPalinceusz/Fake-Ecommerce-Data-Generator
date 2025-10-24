from sqlalchemy import (
    Column, Integer, String, DECIMAL, SmallInteger, Date, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class SysUser(Base):
    __tablename__ = 'SYS_USER'

    USER_ID = Column(Integer, primary_key=True)
    USERNAME = Column(String(50), unique=True, nullable=False)
    PASSWORD_HASH = Column(String(255), nullable=False)
    NAME = Column(String(100))
    SURNAME = Column(String(100))
    EMAIL = Column(String(100))
    ROLE = Column(String(50))
    ACTIVE = Column(SmallInteger, default=1)

    invoices_created = relationship('Invoice', back_populates='created_by_user')


class Customer(Base):
    __tablename__ = 'CUSTOMER'

    CUSTOMER_ID = Column(Integer, primary_key=True)
    NAME = Column(String(100), nullable=False)
    EMAIL = Column(String(100))
    PHONE = Column(String(50))
    ADDRESS = Column(String(255))
    CITY = Column(String(100))
    COUNTRY = Column(String(100))

    orders = relationship('CustomerOrder', back_populates='customer')
    invoices = relationship('Invoice', back_populates='customer')


class Product(Base):
    __tablename__ = 'PRODUCT'

    PRODUCT_ID = Column(Integer, primary_key=True)
    NAME = Column(String(100), nullable=False)
    DESCRIPTION = Column(String(255))
    PRICE = Column(DECIMAL(10, 2), nullable=False)
    STOCK_QUANTITY = Column(Integer, default=0)

    order_items = relationship('OrderItem', back_populates='product')


class CustomerOrder(Base):
    __tablename__ = 'CUSTOMER_ORDER'

    ORDER_ID = Column(Integer, primary_key=True)
    CUSTOMER_ID = Column(Integer, ForeignKey('CUSTOMER.CUSTOMER_ID'), nullable=False)
    ORDER_DATE = Column(DateTime)
    STATUS = Column(String(50), default='PENDING')
    TOTAL_AMOUNT = Column(DECIMAL(10, 2), nullable=False)

    customer = relationship('Customer', back_populates='orders')
    order_items = relationship('OrderItem', back_populates='order')
    invoices = relationship('Invoice', back_populates='order')


class OrderItem(Base):
    __tablename__ = 'ORDER_ITEM'

    ORDER_ITEM_ID = Column(Integer, primary_key=True)
    ORDER_ID = Column(Integer, ForeignKey('CUSTOMER_ORDER.ORDER_ID'), nullable=False)
    PRODUCT_ID = Column(Integer, ForeignKey('PRODUCT.PRODUCT_ID'), nullable=False)
    QUANTITY = Column(Integer, nullable=False)
    UNIT_PRICE = Column(DECIMAL(10, 2), nullable=False)

    order = relationship('CustomerOrder', back_populates='order_items')
    product = relationship('Product', back_populates='order_items')


class Invoice(Base):
    __tablename__ = 'INVOICE'

    INVOICE_ID = Column(Integer, primary_key=True)
    INVOICE_NUMBER = Column(String(50), unique=True, nullable=False)
    CUSTOMER_ID = Column(Integer, ForeignKey('CUSTOMER.CUSTOMER_ID'), nullable=False)
    ORDER_ID = Column(Integer, ForeignKey('CUSTOMER_ORDER.ORDER_ID'))
    ISSUE_DATE = Column(Date, nullable=False)
    DUE_DATE = Column(Date, nullable=False)
    TOTAL_AMOUNT = Column(DECIMAL(10, 2), nullable=False)
    STATUS = Column(String(50), default='UNPAID')
    CREATED_BY = Column(Integer, ForeignKey('SYS_USER.USER_ID'))

    customer = relationship('Customer', back_populates='invoices')
    order = relationship('CustomerOrder', back_populates='invoices')
    created_by_user = relationship('SysUser', back_populates='invoices_created')
    payments = relationship('Payment', back_populates='invoice')


class Payment(Base):
    __tablename__ = 'PAYMENT'

    PAYMENT_ID = Column(Integer, primary_key=True)
    INVOICE_ID = Column(Integer, ForeignKey('INVOICE.INVOICE_ID'), nullable=False)
    PAYMENT_DATE = Column(Date, nullable=False)
    AMOUNT = Column(DECIMAL(10, 2), nullable=False)
    METHOD = Column(String(50))
    CONFIRMED = Column(SmallInteger, default=0)

    invoice = relationship('Invoice', back_populates='payments')
