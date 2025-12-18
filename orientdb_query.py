import pyorient

#ile wyników zapytań zostanie zapisanych
counter = 100

engine = pyorient.OrientDB("localhost", 2424)
engine.set_session_token(True)
session_id = engine.connect("root", "root")
engine.db_open( 'company', "root", "root" )

result1 = engine.query("SELECT NAME , EMAIL FROM SYS_USER WHERE ROLE  = 'ACCOUNTANT'",counter,"*:-1")
for account in result1:
    print(account.NAME)
    print(account.EMAIL)

result2 = engine.query("SELECT COUNT(*) AS NUMBER, CITY  FROM CUSTOMER GROUP BY CITY ",counter,"*:-1")
for city in result2:
    print(city.NUMBER)
    print(city.CITY)

result3 = engine.query("SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000",counter,"*:-1")
for payment in result3:
    print(payment.PAYMENT_ID)
    print(payment.METHOD)
    print(payment.PAYMENT_DATE)
    print(payment.AMOUNT)
    print(payment.CONFIRMED)

result4 = engine.query("SELECT SUM(STOCK_QUANTITY) AS SUMMARY FROM PRODUCT WHERE PRICE < 500",counter,"*:-1")
for stock in result4:
    print(stock.SUMMARY)

result5 = engine.query("SELECT SUM(STOCK_QUANTITY) AS SUMMARY FROM PRODUCT WHERE PRICE < 500",counter,"*:-1")
for stock in result5:
    print(stock.SUMMARY)
engine.db_close()