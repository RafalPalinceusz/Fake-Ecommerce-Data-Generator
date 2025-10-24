import unicodedata
import csv
import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy.orm import Session
from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment


class FakeDataGenerator:
    def __init__(self, session: Session):
        self.fake = Faker('pl_PL')
        self.session = session

        self.USER_ROLES = ["SALES", "ACCOUNTANT", "WAREHOUSE"]
        self.USER_ROLE_WEIGHTS = [0.7, 0.1, 0.2]

        self.CUSTOMER_TYPE_WEIGHTS = [0.6, 0.4]  # 0 = osoba, 1 = firma

        self.ORDER_STATUS_RANDOM_WEIGHTS = [0.7, 0.3]  # 0 = COMPLETED/PROCESSING, 1 = CANCELED/IN_TRANSIT

        self.INVOICE_STATUS_RANDOM_WEIGHTS = [30, 70]  # UNPAID, PAID

        self.PAYMENT_METHODS = ['CREDIT CARD', 'PAYPAL', 'BANK TRANSFER', 'GOOGLE PAY']
        self.PAYMENT_METHOD_WEIGHTS = [40, 30, 20, 10]

    def remove_polish_chars(self, text):
        nfkd_form = unicodedata.normalize('NFKD', text)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    def generate_fake_products(self):
        products = []
        with open('products.csv', mode='r', encoding="utf-8") as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                product = Product(
                    NAME=lines[0],
                    DESCRIPTION=lines[1].replace('"', ''),
                    PRICE=round(self.fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
                    STOCK_QUANTITY=random.randint(1, 500),
                )
                products.append(product)
        return products

    def generate_fake_users(self, count: int):
        users = []
        for i in range(count):
            name = self.fake.first_name()
            surname = self.fake.last_name()
            username = self.remove_polish_chars(name) + self.remove_polish_chars(surname) + str(random.randint(1, 100))
            user = SysUser(
                USERNAME=username,
                PASSWORD_HASH=self.fake.sha256(),
                NAME=name,
                SURNAME=surname,
                EMAIL=username + '@mycompany.com',
                ROLE=random.choices(self.USER_ROLES, weights=self.USER_ROLE_WEIGHTS, k=1)[0]
            )
            users.append(user)
        return users

    def generate_fake_customers(self, count: int):
        customers = []
        for i in range(count):
            if random.choices([0, 1], weights=self.CUSTOMER_TYPE_WEIGHTS, k=1)[0] == 0:
                name = self.fake.first_name() + ' ' + self.fake.last_name()
                email = name.replace(' ', '').lower() + '@customer.pl'
            else:
                name = self.fake.company()
                domain = name.lower().replace(" ", "").replace('.', "") + ".com"
                email = f"contact@{domain}"

            customer = Customer(
                NAME=name,
                EMAIL=email,
                PHONE=self.fake.phone_number(),
                ADDRESS=f"{self.fake.street_name()} {self.fake.building_number()}, {self.fake.postcode()}",
                CITY=self.fake.city(),
                COUNTRY="Polska",
            )
            customers.append(customer)
        return customers

    def generate_order_items(self, products):
        items = []
        count = random.randint(1, 15)
        rd_products = random.choices(products, k=count)
        for product in rd_products:
            order_item = OrderItem(
                ORDER_ID=0,
                PRODUCT_ID=product.PRODUCT_ID,
                QUANTITY=random.randint(1, 10),
                UNIT_PRICE=product.PRICE
            )
            items.append(order_item)
        return items

    def generate_fake_order_data(self, customers, products, sales_users):
        start_datetime = datetime(2022, 1, 1, 0, 0, 0)
        end_datetime = datetime(2025, 9, 30, 23, 59, 59)
        random_dt = self.fake.date_time_between(start_date=start_datetime, end_date=end_datetime)
        days_diff = (end_datetime - random_dt).days
        rd_status = random.choices([0, 1], weights=self.ORDER_STATUS_RANDOM_WEIGHTS, k=1)[0]

        if days_diff > 60:
            status = 'COMPLETED' if rd_status == 0 else 'CANCELED'
        elif days_diff < 3:
            status = 'PENDING'
        else:
            status = 'PROCESSING' if rd_status == 0 else 'IN_TRANSIT'

        order_items = self.generate_order_items(products)
        total_amount = sum(item.UNIT_PRICE * item.QUANTITY for item in order_items)

        order = CustomerOrder(
            CUSTOMER_ID=random.choice(customers).CUSTOMER_ID,
            ORDER_DATE=random_dt,
            STATUS=status,
            TOTAL_AMOUNT=total_amount,
        )
        # self.session.add(order)
        # self.session.flush()


        # for item in order_items:
        #     item.ORDER_ID = order.ORDER_ID
        # self.session.add_all(order_items)
        order.order_items = order_items
        self.session.add(order)

        invoice_creator = random.choice(sales_users)
        self.generate_fake_invoice(order, invoice_creator)

    def generate_fake_invoice(self, order, user):
        random_suffix = random.randint(100000, 999999)
        invoice_number = f"FV/{order.ORDER_DATE.strftime('%Y%m%d')}/{random_suffix}"
        issue_date = order.ORDER_DATE
        due_date = issue_date + timedelta(days=14)
        total_amount = order.TOTAL_AMOUNT

        if order.STATUS == 'COMPLETED':
            status = 'PAID'
        elif order.STATUS == 'CANCELED':
            status = 'CANCELED'
        elif order.STATUS == 'PENDING':
            status = 'UNPAID'
        else:
            status = random.choices(['UNPAID', 'PAID'], weights=self.INVOICE_STATUS_RANDOM_WEIGHTS, k=1)[0]

        created_by = user.USER_ID
        invoice = Invoice(
            INVOICE_NUMBER=invoice_number,
            STATUS=status,
            CUSTOMER_ID=order.CUSTOMER_ID,
            ISSUE_DATE=issue_date,
            DUE_DATE=due_date,
            TOTAL_AMOUNT=total_amount,
            CREATED_BY=created_by,
        )
        order.invoices.append(invoice)

        if status == 'PAID':
            payment_date = order.ORDER_DATE + timedelta(days=random.randint(0, 6))
            method = random.choices(self.PAYMENT_METHODS, weights=self.PAYMENT_METHOD_WEIGHTS, k=1)[0]

            payment = Payment(
                PAYMENT_DATE=payment_date,
                AMOUNT=order.TOTAL_AMOUNT,
                METHOD=method,
                CONFIRMED=1
            )
            invoice.payments.append(payment)

    def run_generation(self, num_users: int, num_customers: int, num_orders: int):
        print("Start: Generowanie danych...")

        products = self.generate_fake_products()
        print(f"Wygenerowano {len(products)} produktów z CSV. Zapisywanie (bulk)...")
        self.session.bulk_save_objects(products, return_defaults=True)

        users = self.generate_fake_users(num_users)
        print(f"Wygenerowano {len(users)} użytkowników. Zapisywanie (bulk)...")
        self.session.bulk_save_objects(users, return_defaults=True)

        customers = self.generate_fake_customers(num_customers)
        print(f"Wygenerowano {len(customers)} klientów. Zapisywanie (bulk)...")
        self.session.bulk_save_objects(customers, return_defaults=True)


        sales_users = [u for u in users if u.ROLE == 'SALES']
        if not sales_users:
            print("BŁĄD: Nie znaleziono użytkowników z rolą 'SALES'. Nie można wygenerować faktur.")
            print("Anulowanie.")
            self.session.rollback()
            return

        print(f"Znaleziono {len(sales_users)} użytkowników 'SALES' do wystawiania faktur.")

        print(f"Start: Generowanie {num_orders} zamówień...")
        for i in range(num_orders):
            self.generate_fake_order_data(customers, products, sales_users)
            if (i + 1) % 100 == 0:
                print(f" ... wygenerowano {i + 1}/{num_orders} zamówień")

        try:
            print("Commit: Zapisywanie wszystkich zmian w bazie danych...")
            self.session.commit()
            print("Gotowe. Dane zostały wygenerowane pomyślnie.")
        except Exception as e:
            print(f"BŁĄD: Wystąpił błąd podczas commita. Wycofywanie zmian. {e}")
            self.session.rollback()
        finally:
            self.session.close()