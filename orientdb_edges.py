import pyorient

orient_engine = pyorient.OrientDB("localhost", 2424)
orient_engine.set_session_token(True)
session_id = orient_engine.connect("root", "root")
orient_engine.db_open( 'company', "root", "root" )

orient_engine.command("CREATE CLASS Customer_to_invoice EXTENDS E;")
orient_engine.command("CREATE CLASS Invoice_to_payment EXTENDS E;")

Customer_to_invoice= "LET customers = (SELECT FROM CUSTOMER); FOREACH (c IN $customers) {CREATE EDGE Customer_to_invoice FROM $c TO (SELECT FROM INVOICE WHERE CUSTOMER_ID = $c.CUSTOMER_ID);}"
orient_engine.command(Customer_to_invoice)

Invoice_to_payment = "LET invoices = (SELECT FROM INVOICE); FOREACH (i IN $invoices) { CREATE EDGE Invoice_to_payment FROM $i TO (SELECT FROM PAYMENT WHERE INVOICE_ID = $i.INVOICE_ID);}"
orient_engine.command(Invoice_to_payment)

orient_engine.db_close()