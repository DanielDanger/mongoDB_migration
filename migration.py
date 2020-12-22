import mysql.connector
import datetime
from pymongo import MongoClient
from random import randint
from pymongo import MongoClient
from decimal import Decimal
from bson.decimal128 import Decimal128

def convert_decimal(dict_item):
    # This function iterates a dictionary looking for types of Decimal and converts them to Decimal128
    # Embedded dictionaries and lists are called recursively.
    if dict_item is None: return None

    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            convert_decimal(v)
        elif isinstance(v, list):
            for l in v:
                convert_decimal(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))

    return dict_item



def migrate_orders(): 
    cnx = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    cnx2 = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    orders = cnx.cursor()
    query = ("select * from orders")
    orders.execute(query)
    

    client = MongoClient(host='192.168.56.101',port=27017)
    db=client.salesdb
    for (orderNumber,OrderDate,requiredDate,shippedDate,status,comments,customerNumber) in orders: 
        if (shippedDate == None):
            shippedDate = datetime.datetime.strptime('18/09/19 01:55:19', '%d/%m/%y %H:%M:%S')
        orderDetails = cnx2.cursor()
        query = ("select * from orderdetails where orderNumber = {}".format(orderNumber))
        orderDetails.execute(query)
        details = []
        for (orderNumber,productCode,quantityOrdered,priceEach,orderLineNumber) in orderDetails:
            detail = {
                "productCode": productCode,
                "quantityOrdered": quantityOrdered,
                "priceEach": priceEach,
                "orderLineNumber": orderLineNumber
            }
            details.append(detail)
        order = {
            "orderNumber": orderNumber,
            "OrderDate": datetime.datetime.combine(OrderDate, datetime.time.min),
            "requiredDate": datetime.datetime.combine(requiredDate, datetime.time.min),
            "shippedDate": datetime.datetime.combine(shippedDate, datetime.time.min),
            "status": status,
            "comments": comments,
            "customerNumber": customerNumber,
            "orderDetails": details
        }
        db.orders.insert_one(convert_decimal(order))
    cnx.close()
    cnx2.close()

def migrate_products(): 
    cnx = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    cnx2 = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    products = cnx.cursor()
    query = ("select * from products")
    products.execute(query)

    client = MongoClient(host='192.168.56.101',port=27017)
    db=client.salesdb
    for (productCode,productName,productLine,productScale,productVendor,productDescription,quantityInStock, buyPrice, MSRP) in products: 
        productLineCursor = cnx2.cursor()
        query = ("select * from productlines where productLine = '{}'".format(productLine))
        productLineCursor.execute(query)
        productLineObject = None
        for (productLine,textDescription,htmlDescription,image) in productLineCursor:
            productLineObject = {
                "productLine": productLine,
                "textDescription": textDescription,
                "htmlDescription": htmlDescription,
                "image": image
            }
        product = {
            "productCode": productCode,
            "productName": productName,
            "productLine": productLineObject,
            "productScale": productScale,
            "productVendor": productVendor,
            "productDescription": productDescription,
            "quantityInStock": quantityInStock,
            "buyPrice": buyPrice,
            "MSRP": MSRP
        }
        db.products.insert_one(convert_decimal(product))
    cnx.close()
    cnx2.close()

def migrate_customers(): 
    cnx = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    cnx2 = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')
    customers = cnx.cursor()
    query = ("select * from customers")
    customers.execute(query)
    

    client = MongoClient(host='192.168.56.101',port=27017)
    db=client.salesdb
    for (customerNumber,customerName,contactLastName,contactFirstName,phone,addressLine1,addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit) in customers: 
        payments = cnx2.cursor()
        query = ("select * from payments where customerNumber = {}".format(customerNumber))
        payments.execute(query)
        paymentsArray = []
        for (customerNumber,checkNumber,paymentDate,amount) in payments:
            payment = {
                "checkNumber": checkNumber,
                "paymentDate": datetime.datetime.combine(paymentDate, datetime.time.min),
                "amount": amount
            }
            paymentsArray.append(payment)
        customer = {
            "customerNumber": customerNumber,
            "customerName": customerName,
            "contactLastName": contactLastName,
            "contactFirstName": contactFirstName,
            "phone": phone,
            "addressLine1": addressLine1,
            "addressLine2": addressLine2,
            "city": city,
            "state": state,
            "postalCode": postalCode,
            "country": country,
            "salesRepEmployeeNumber": salesRepEmployeeNumber,
            "creditLimit": creditLimit,
            "payments": paymentsArray
        }
        db.customers.insert_one(convert_decimal(customer))
    cnx.close()
    cnx2.close()

def migrate_offices(): 
    cnx = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')

    offices = cnx.cursor()
    query = ("select * from offices")
    offices.execute(query)
    

    client = MongoClient(host='192.168.56.101',port=27017)
    db=client.salesdb
    for (officeCode, city, phone, addressLine1, addressLine2, state, country, postalCode, territory) in offices: 
        office = {
            "officeCode": officeCode,
            "phone": phone,
            "addressLine1": addressLine1,
            "addressLine2": addressLine2,
            "city": city,
            "state": state,
            "postalCode": postalCode,
            "country": country,
            "territory": territory
        }
        db.offices.insert_one(convert_decimal(office))
    cnx.close()

def migrate_employees(): 
    cnx = mysql.connector.connect(user='dbuser', password='dbuser',
                              host='192.168.56.101',
                              database='salesdb')

    employees = cnx.cursor()
    query = ("select * from employees")
    employees.execute(query)
    

    client = MongoClient(host='192.168.56.101',port=27017)
    db=client.salesdb
    for (employeeNumber, lastName, firstName, extension, email, officeCode, reportsTo, jobTitle) in employees: 
        employee = {
            "employeeNumber": employeeNumber,
            "lastName": lastName,
            "firstName": firstName,
            "extension": extension,
            "email": email,
            "officeCode": officeCode,
            "reportsTo": reportsTo,
            "jobTitle": jobTitle
        }
        db.employees.insert_one(convert_decimal(employee))
    cnx.close()
  
def migrate_salesdb(): 
    migrate_orders()
    migrate_products()
    migrate_customers()
    migrate_offices()
    migrate_employees()

migrate_salesdb()










# ##### Verbindung zur Mongo DB 

# client = MongoClient(host='192.168.56.101',port=27017)


# db=client.business
# #Step 2: Create sample data
# names = ['Kitchen','Animal','State', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
# company_type = ['LLC','Inc','Company','Corporation']
# company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
# for x in range(1, 501):
#     business = {
#         'name' : names[randint(0, (len(names)-1))] + ' ' + names[randint(0, (len(names)-1))]  + ' ' + company_type[randint(0, (len(company_type)-1))],
#         'rating' : randint(1, 5),
#         'cuisine' : company_cuisine[randint(0, (len(company_cuisine)-1))] 
#     }
#     #Step 3: Insert business object directly into MongoDB via isnert_one
#     result=db.reviews.insert_one(business)
#     #Step 4: Print to the console the ObjectID of the new document
#     print('Created {0} of 500 as {1}'.format(x,result.inserted_id))
# #Step 5: Tell us that you are done
# print('finished creating 500 business reviews')
