from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra import ConsistencyLevel


# ==========================================
# 1. Proste filtrowanie (WHERE ROLE = ...)
# SQL: SELECT NAME, EMAIL FROM SYS_USER WHERE ROLE = 'ACCOUNTANT'
# ==========================================
class UsersByRole(Model):
    __table_name__ = 'users_by_role'

    # Partition Key: Grupujemy użytkowników po roli, żeby pobrać wszystkich 'ACCOUNTANT' jednym strzałem
    role = columns.Text(partition_key=True)

    # Clustering Key: Unikalność wewnątrz roli zapewnia ID
    user_id = columns.Integer(primary_key=True)

    # Dane
    username = columns.Text()
    name = columns.Text()
    email = columns.Text()


# ==========================================
# 2. Grupowanie (GROUP BY CITY)
# SQL: SELECT COUNT(*), CITY FROM CUSTOMER GROUP BY CITY
# ==========================================
class CustomersByCity(Model):
    __table_name__ = 'customers_by_city'

    # Partition Key: Wszystkich klientów z jednego miasta trzymamy razem
    city = columns.Text(partition_key=True)

    # Clustering Key: ID klienta
    customer_id = columns.Integer(primary_key=True)

    # Dane pomocnicze (można dodać imię, żeby wyświetlić listę bez dodatkowych zapytań)
    name = columns.Text()
    email = columns.Text()


# ==========================================
# 3. Zakresy liczbowe - Płatności (BETWEEN)
# SQL: SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000
# ==========================================
class PaymentsByYearAmount(Model):
    __table_name__ = 'payments_by_year_amount'

    # Partition Key: Musimy dodać sztuczny podział (np. rok), żeby nie skanować całej bazy
    year = columns.Integer(partition_key=True)

    # Clustering Key: Sortujemy po kwocie, żeby działało zapytanie zakresowe (BETWEEN)
    amount = columns.Decimal(primary_key=True, clustering_order="ASC")
    payment_id = columns.Integer(primary_key=True)

    # Dane
    method = columns.Text()
    payment_date = columns.DateTime()
    confirmed = columns.Boolean()


# ==========================================
# SQL: SELECT SUM(STOCK_QUANTITY) FROM PRODUCT WHERE PRICE < 100
# ==========================================
class ProductsByPrice(Model):
    __table_name__ = 'products_by_price'
    bucket = columns.Text(partition_key=True, default="all_products")
    price = columns.Decimal(primary_key=True, clustering_order="ASC")
    product_id = columns.Integer(primary_key=True)
    name = columns.Text()
    stock_quantity = columns.Integer()


# ==========================================
# SQL: JOINY oraz UPDATE/ALTER
# ==========================================
class InvoiceFullDetails(Model):
    __table_name__ = 'invoice_full_details'

    invoice_id = columns.Integer(partition_key=True)

    invoice_number = columns.Text()
    issue_date = columns.Date()
    due_date = columns.Date()
    total_amount = columns.Decimal()
    status = columns.Text()

    past_due = columns.Boolean(default=False)

    customer_id = columns.Integer()
    customer_name = columns.Text()
    customer_email = columns.Text()


    payment_method = columns.Text()
    payment_amount = columns.Decimal()
    payment_confirmed = columns.Boolean()


# ==========================================
# Agregacja: Sprzedaż wg Kraju i Produktu
# ==========================================
class SalesStatsByCountry(Model):
    __table_name__ = 'sales_stats_by_country_product'

    # Partition Key: Analiza per kraj
    country = columns.Text(partition_key=True)

    # Clustering Keys: Sortowanie po ilości (malejąco) - to robi robotę "ORDER BY"
    total_quantity_sum = columns.Integer(primary_key=True, clustering_order="DESC")
    product_name = columns.Text(primary_key=True, clustering_order="ASC")

    # Można dodać product_id dla referencji
    product_id = columns.Integer()


# ==========================================
# Agregacja: Customer 360 - bardzo szeroka tabela
# ==========================================
class CustomerLeaderboard(Model):
    __table_name__ = 'customer_performance_leaderboard'

    country = columns.Text(partition_key=True)

    gross_value_brutto = columns.Decimal(primary_key=True, clustering_order="DESC")

    customer_name = columns.Text(primary_key=True, clustering_order="ASC")

    agent_username = columns.Text()
    orders_count = columns.Integer()
    unique_products_count = columns.Integer()
    total_items_quantity = columns.Integer()
    last_invoice_date = columns.Date()


# ==========================================
# Funkcja inicjalizująca bazę
# ==========================================
def init_cassandra_schema(keyspace='my_keyspace', nodes=['127.0.0.1']):
    print(f"1. Łączenie z klastrem Cassandra: {nodes}...")

    # KROK 1: Łączymy się bez wskazywania keyspace (lub do domyślnego 'system'),
    # żeby móc wykonać operacje administracyjne.
    # UWAGA: Jeśli nadal masz błąd z 'asyncore', dodaj tu connection_class=AsyncioConnection jak ustaliliśmy wcześniej.
    connection.setup(nodes, 'system', protocol_version=4)

    print(f"2. Tworzenie Keyspace '{keyspace}' (jeśli nie istnieje)...")
    # KROK 2: Tworzymy Keyspace.
    # replication_factor=1 jest KLUCZOWE dla Twojego docker-compose (masz 1 węzeł).
    # Jeśli ustawisz więcej, Cassandra będzie czekać na nienarodzone węzły.
    create_keyspace_simple(keyspace, replication_factor=1)

    print(f"3. Ustawianie domyślnego Keyspace na '{keyspace}'...")
    # KROK 3: Teraz ustawiamy połączenie na właściwy keyspace
    connection.setup(nodes, keyspace, protocol_version=4)

    print("4. Synchronizacja tabel (tworzenie struktur)...")
    # KROK 4: Tworzymy tabele
    sync_table(UsersByRole)
    sync_table(CustomersByCity)
    sync_table(PaymentsByYearAmount)
    sync_table(ProductsByPrice)
    sync_table(InvoiceFullDetails)
    sync_table(SalesStatsByCountry)
    sync_table(CustomerLeaderboard)

    print("Gotowe! Struktura Cassandry została zainicjowana.")