from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from FakeDataGenerator import FakeDataGenerator
from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment
from datetime import date, datetime
import encodings
import codecs

# rejestracja kodowania cp1250, wyrzucało błąd dla firebirda
try:
    codecs.lookup('cp1250')
except LookupError:
    import encodings.cp1250


engine = create_engine(
    "firebird+firebird://sysdba:admin123@localhost:3050//var/lib/firebird/data/mirror.fdb?charset=UTF8",
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_reset_on_return=None
)

print("Czyszczenie bazy danych (DROP ALL)...")
Base.metadata.drop_all(bind=engine)
print("Tworzenie bazy danych od nowa (CREATE ALL)...")
Base.metadata.create_all(bind=engine)
print("Baza danych jest czysta i gotowa.")

Session = sessionmaker(bind=engine)
session = Session()

generator = FakeDataGenerator(session)

generator.run_generation(num_users=10, num_customers=10, num_orders=10)

