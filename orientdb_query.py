import pyorient

#ile wyników zapytań zostanie zapisanych
counter = 100

orient_engine = pyorient.OrientDB("localhost", 2424)
orient_engine.set_session_token(True)
session_id = orient_engine.connect("root", "root")
orient_engine.db_open( 'company', "root", "root" )

result1 = orient_engine.query("SELECT NAME , EMAIL FROM SYS_USER WHERE ROLE  = 'ACCOUNTANT'",counter,"*:-1")
for account in result1:
    print(account.NAME)
    print(account.EMAIL)

result2 = orient_engine.query("SELECT COUNT(*) AS NUMBER, CITY  FROM CUSTOMER GROUP BY CITY ",counter,"*:-1")
for city in result2:
    print(city.NUMBER)
    print(city.CITY)

result3 = orient_engine.query("SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000",counter,"*:-1")
for payment in result3:
    print(payment.PAYMENT_ID)
    print(payment.METHOD)
    print(payment.PAYMENT_DATE)
    print(payment.AMOUNT)
    print(payment.CONFIRMED)

result4 = orient_engine.query("SELECT SUM(STOCK_QUANTITY) AS SUMMARY FROM PRODUCT WHERE PRICE < 500",counter,"*:-1")
for stock in result4:
    print(stock.SUMMARY)

result5 = orient_engine.query("MATCH {class: CUSTOMER, as: c} -Customer_to_invoice-> {class: INVOICE, as: i} RETURN i.CUSTOMER_ID , i.INVOICE_NUMBER, c.NAME",counter,"*:-1")
for invoice in result5:
    print(invoice.i.CUSTOMER_ID)
    print(invoice.i.INVOICE_NUMBER)
    print(invoice.c.NAME)
orient_engine.db_close()

result6 = orient_engine.query("MATCH {class: INVOICE, as: i} -Invoice_to_payment-> {class: PAYMENT, as: p}RETURN i.INVOICE_ID , i.INVOICE_NUMBER , p.METHOD  , p.AMOUNT ORDER BY i.INVOICE_ID",counter,"*:-1")
for payment in result6:
    print(invoice.i.INVOICE_ID)
    print(invoice.i.INVOICE_NUMBER)
    print(invoice.p.METHOD)
    print(invoice.p.AMOUNT)
orient_engine.db_close()

#Query 7 nie musi być wykonywane ze względu na nieschematycze działanie OrientDB, pozwaljące dodawać nowe pola  od ręki