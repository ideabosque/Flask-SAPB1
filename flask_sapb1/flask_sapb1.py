from flask import current_app
import pymssql
import datetime
from time import time
import decimal

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack


class Adaptor(object):
    """Adaptor contains SAP B1 COM object and MS SQL cursor object.
    """

    def __init__(self, company=None, sqlSrvConn=None):
        self._company = company
        self._sqlSrvConn = sqlSrvConn
        self._sqlSrvCursor = self._sqlSrvConn.cursor(as_dict=True)

    def __del__(self):
        if self._company:
            self._company.Disconnect()
        self._sqlSrvConn.close()

    @property
    def company(self):
        """SAPB1 COM object.
        """
        return self._company

    @property
    def sqlSrvCursor(self):
        """MS SQL Server cursor object.
        """
        return self._sqlSrvCursor

    def disconnect(self):
        self._company.Disconnect()
        self._sqlSrvConn.close()


class SAPB1Adaptor(object):
    """SAP B1 Adaptor with functions.
    """

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Use the newstyle teardown_appcontext if it's available,
        otherwise fall back to the request context
        """
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    def connect(self):
        """Initiate the connect with SAP B1 and MS SQL server.
        """
        SAPbobsCOM = __import__(current_app.config['DIAPI'], globals(), locals(), [], -1)
        self.constants = getattr(SAPbobsCOM, "constants")
        Company = getattr(SAPbobsCOM, "Company")
        company = Company()
        company.Server = current_app.config['SERVER']
        company.UseTrusted = False
        company.language = eval("self.constants." + current_app.config['LANGUAGE'])
        company.DbServerType = eval("self.constants." + current_app.config['DBSERVERTYPE'])
        company.CompanyDB = current_app.config['COMPANYDB']
        company.UserName = current_app.config['B1USERNAME']
        company.Password = current_app.config['B1PASSWORD']
        company.Connect()
        log = "Open SAPB1 connection for " + company.CompanyName
        current_app.logger.info(log)

        sqlSrvConn = pymssql.connect(current_app.config['SERVER'],
                                    current_app.config['DBUSERNAME'],
                                    current_app.config['DBPASSWORD'],
                                    current_app.config['COMPANYDB'])
        log = "Open SAPB1 DB connection"
        current_app.logger.info(log)
        return Adaptor(company, sqlSrvConn)

    def teardown(self, exception):
        ctx = stack.top
        if hasattr(ctx, 'sapb1adaptor'):
            ctx.sapb1adaptor.disconnect()

    def info(self):
        """Show the information for the SAP B1 connection.
        """
        data = {
            'company_name': self.adaptor.company.CompanyName,
            'diapi': current_app.config['DIAPI'],
            'server': current_app.config['SERVER'],
            'company_db': current_app.config['COMPANYDB']
        }
        return data

    @property
    def adaptor(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'sapb1adaptor'):
                ctx.sapb1adaptor = self.connect()
            return ctx.sapb1adaptor

    def trimValue(self, value, maxLength):
        """Trim the value.
        """
        if len(value) > maxLength:
            return value[0:maxLength-1]
        return value

    def getOrders(self, num=1, columns=[], params={}):
        """Retrieve orders from SAP B1.
        """
        cols = '*'
        if len(columns) > 0:
            cols = " ,".join(columns)
        ops = {key: '=' if 'op' not in params[key].keys() else params[key]['op'] for key in params.keys()}
        sql = """SELECT top {0} {1} FROM dbo.ORDR""".format(num, cols)
        if len(params) > 0:
            sql = sql + ' WHERE ' + " AND ".join(["{0} {1} %({2})s".format(k, ops[k], k) for k in params.keys()])
        self.adaptor.sqlSrvCursor.execute(sql, {key: params[key]['value'] for key in params.keys()})
        orders = []
        for row in self.adaptor.sqlSrvCursor:
            order = {}
            for k, v in row.items():
                value = ''
                if type(v) is datetime.datetime:
                    value = v.strftime("%Y-%m-%d %H:%M:%S")
                elif v is not None:
                    value = str(v)
                order[k] = value
            orders.append(order)
        return orders

    def getMainCurrency(self):
        """Retrieve the main currency of the company from SAP B1.
        """
        sql = """SELECT MainCurncy FROM dbo.OADM"""
        self.adaptor.sqlSrvCursor.execute(sql)
        mainCurrency = self.adaptor.sqlSrvCursor.fetchone()['MainCurncy']
        return mainCurrency

    def getContacts(self, num=1, columns=[], cardCode=None, contact={}):
        """Retrieve contacts under a business partner by CardCode from SAP B1.
        """
        cols = '*'
        if len(columns) > 0:
            cols = " ,".join(columns)

        sql = """SELECT top {0} {1} FROM dbo.OCPR""".format(num, cols)
        params = dict({(k, 'null' if v is None else v) for k, v in contact.items()})
        params['cardcode'] = cardCode
        sql = sql + ' WHERE ' + " AND ".join(["{0} = %({1})s".format(k, k) for k in params.keys()])

        self.adaptor.sqlSrvCursor.execute(sql, params)
        contacts = []
        for row in self.adaptor.sqlSrvCursor:
            contact = {}
            for k, v in row.items():
                value = ''
                if type(v) is datetime.datetime:
                    value = v.strftime("%Y-%m-%d %H:%M:%S")
                elif v is not None:
                    value = str(v)
                contact[k] = value
            contacts.append(contact)
        return contacts

    def insertContact(self, cardCode, contact):
        """Insert a new contact into a business partner by CardCode.
        """
        busPartner = self.adaptor.company.GetBusinessObject(self.constants.oBusinessPartners)
        busPartner.GetByKey(cardCode)
        current = busPartner.ContactEmployees.Count
        if busPartner.ContactEmployees.InternalCode == 0:
            nextLine = 0
        else :
            nextLine = current
        busPartner.ContactEmployees.Add()
        busPartner.ContactEmployees.SetCurrentLine(nextLine)
        name = contact['FirstName'] + ' ' + contact['LastName']
        name = name[0:36] + ' ' + str(time())
        busPartner.ContactEmployees.Name = name
        busPartner.ContactEmployees.FirstName = contact['FirstName']
        busPartner.ContactEmployees.LastName = contact['LastName']
        busPartner.ContactEmployees.Phone1 = contact["Tel1"]
        busPartner.ContactEmployees.E_Mail = contact["E_MailL"]
        address = contact['Address']
        busPartner.ContactEmployees.Address = self.trimValue(address,100)
        lRetCode = busPartner.Update()
        if lRetCode != 0:
            log = self.company.GetLastErrorDescription()  # self.company.GetLastError()
            current_app.logger.error(log)
            raise Exception(log)

        cntct = {
            "name": name,
            "FirstName": contact['FirstName'],
            "LastName": contact['LastName'],
            "E_MailL": contact["E_MailL"]
        }
        contacts = self.getContacts(num=1, columns=['cntctcode'], cardCode=cardCode, contact=cntct)
        contactCode = contacts[0]['cntctcode']
        return contactCode

    def getContactPersonCode(self, order):
        """Retrieve ContactPersonCode by an order.
        """
        contact = {
            'FirstName': order['billto_firstname'],
            'LastName': order['billto_lastname'],
            'E_MailL': order['billto_email']
        }
        contacts = self.getContacts(num=1, columns=['cntctcode'], cardCode=order['card_code'], contact=contact)
        contactCode = None
        if len(contacts) == 1:
            contactCode = contacts[0]['cntctcode']
        if contactCode is None:
            address = order['billto_address'] + ', ' \
                      + order['billto_city'] + ', ' \
                      + order['billto_state'] + ' ' \
                      + order['billto_zipcode'] + ', ' \
                      + order['billto_country']
            contact['Address'] = self.trimValue(address,100)
            contact['Tel1'] = order['billto_telephone']
            contactCode = self.insertContact(order['card_code'], contact)
        return contactCode

    def getExpnsCode(self, expnsName):
        """Retrieve expnsCode by expnsName.
        """
        sql = """SELECT ExpnsCode FROM dbo.OEXD WHERE ExpnsName = %s"""
        self.adaptor.sqlSrvCursor.execute(sql, (expnsName))
        expnsCode = self.adaptor.sqlSrvCursor.fetchone()['ExpnsCode']
        return expnsCode

    def getTrnspCode(self, trnspName):
        """Retrieve TrnspCode by trnspName.
        """
        sql = """SELECT TrnspCode FROM dbo.OSHP WHERE TrnspName = %s"""
        self.adaptor.sqlSrvCursor.execute(sql, (trnspName))
        trnspCode = self.adaptor.sqlSrvCursor.fetchone()['TrnspCode']
        return trnspCode

    def insertOrder(self, o):
        """Insert an order into SAP B1.
        """
        o["billto_telephone"] = self.trimValue(o["billto_telephone"],20)
        o['billto_address'] = self.trimValue(o['billto_address'],100)
        o['shipto_address'] = self.trimValue(o['shipto_address'],100)
        order = self.adaptor.company.GetBusinessObject(self.constants.oOrders)
        order.DocDueDate = o['doc_due_date']
        order.CardCode = o['card_code']
        name = o['billto_firstname'] + ' ' + o['billto_lastname']
        name = name[0:50]
        order.CardName = name
        order.DocCurrency = self.getMainCurrency()
        order.ContactPersonCode = self.getContactPersonCode(o)
        if 'expenses_freightname' in o.keys():
            order.Expenses.ExpenseCode = self.getExpnsCode(o['expenses_freightname'])
            order.Expenses.LineTotal = o['expenses_linetotal']
            order.Expenses.TaxCode = o['expenses_taxcode']
        if 'discount_percent' in o.keys():
            order.DiscountPercent = o['discount_percent']

        # Set Shipping Type
        if 'transport_name' in o.keys():
            order.TransportationCode = self.getTrnspCode(o['transport_name'])

        # Set Payment Method
        if 'payment_method' in o.keys():
            order.PaymentMethod = o['payment_method']

        # Set Magento Order Inc Id
        if 'fe_order_id_udf' in o.keys():
            order.UserFields.Fields.Item(o['fe_order_id_udf']).Value = str(o['fe_order_id'])
        else:
            order.NumAtCard = str(o['fe_order_id'])

        # Set bill to address properties
        order.AddressExtension.BillToCity = o['billto_city']
        order.AddressExtension.BillToCountry = o['billto_country']
        order.AddressExtension.BillToCounty = o['billto_country']
        order.AddressExtension.BillToState = o['billto_state']
        order.AddressExtension.BillToStreet = o['billto_address']
        order.AddressExtension.BillToZipCode = o['billto_zipcode']

        # Set ship to address properties
        order.AddressExtension.ShipToCity = o['shipto_city']
        order.AddressExtension.ShipToCountry = o['shipto_country']
        order.AddressExtension.ShipToCounty = o['shipto_county']
        order.AddressExtension.ShipToState = o['shipto_state']
        order.AddressExtension.ShipToStreet = o['shipto_address']
        order.AddressExtension.ShipToZipCode = o['shipto_zipcode']

        i = 0
        for item in o['items']:
            order.Lines.Add()
            order.Lines.SetCurrentLine(i)
            order.Lines.ItemCode = item['itemcode']
            order.Lines.Quantity = float(item['quantity'])
            order.Lines.Price = decimal.Decimal(item['price'])
            order.Lines.TaxCode = item['taxcode']
            order.Lines.LineTotal = item['linetotal']
            i = i + 1

        lRetCode = order.Add()
        if lRetCode != 0:
            error = str(self.adaptor.company.GetLastError())
            current_app.logger.error(error)
            raise Exception(error)
        else:
            params = None
            if 'fe_order_id_udf' in o.keys():
                params = {o['fe_order_id_udf']: {'value': str(o['fe_order_id'])}}
            else:
                params = {'NumAtCard': {'value': str(o['fe_order_id'])}}
            orders = self.getOrders(num=1, columns=['DocEntry'], params=params)
            boOrderId = orders[0]['DocEntry']
            return boOrderId

    def cancelOrder(self,o):
        """Cancel an order in SAP B1.
        """
        order = self.adaptor.company.GetBusinessObject(self.constants.oOrders)
        params = None
        if 'fe_order_id_udf' in o.keys():
            params = {o['fe_order_id_udf']: {'value': str(o['fe_order_id'])}}
        else:
            params = {'NumAtCard': {'value': str(o['fe_order_id'])}}
        orders = self.getOrders(num=1, columns=['DocEntry'], params=params)
        if orders:
            boOrderId = orders[0]['DocEntry']
            order.GetByKey(boOrderId)
            lRetCode = order.Cancel()
            if lRetCode != 0:
                error = str(self.company.GetLastError())
                self.logger.error(error)
                raise Exception(error)
            else :
                return boOrderId
        else :
            raise Exception("Order {0} is not found.".format(o['fe_order_id']))

    def _getShipmentItems(self, shipmentId, columns=[]):
        """Retrieve line items for each shipment(delivery) from SAP B1.
        """
        cols = "*"
        if len(columns) > 0:
            cols = " ,".join(columns)
        sql = """SELECT {0} FROM dbo.DLN1""".format(cols)
        params = {
            'DocEntry' : shipmentId
        }
        if len(params) > 0:
            sql = sql + ' WHERE ' + " AND ".join(["{0} = %({1})s".format(k, k) for k in params.keys()])

        self.adaptor.sqlSrvCursor.execute(sql, params)
        items = []
        for row in self.adaptor.sqlSrvCursor:
            item = {}
            for k, v in row.items():
                value = ''
                if type(v) is datetime.datetime:
                    value = v.strftime("%Y-%m-%d %H:%M:%S")
                elif v is not None:
                    value = str(v)
                item[k] = value
            items.append(item)
        return items

    def getShipments(self, num=100, columns=[], params={}, itemColumns=[]):
        """Retrieve shipments(deliveries) from SAP B1.
        """
        cols = '*'
        if 'DocEntry' not in columns:
            columns.append('DocEntry')
        if len(columns) > 0:
            cols = " ,".join(columns)
        ops = {key: '=' if 'op' not in params[key].keys() else params[key]['op'] for key in params.keys()}
        sql = """SELECT top {0} {1} FROM dbo.ODLN""".format(num, cols)
        if len(params) > 0:
            sql = sql + ' WHERE ' + " AND ".join(["{0} {1} %({2})s".format(k, ops[k], k) for k in params.keys()])
        self.adaptor.sqlSrvCursor.execute(sql, {key: params[key]['value'] for key in params.keys()})
        shipments = []
        for row in self.adaptor.sqlSrvCursor:
            shipment = {}
            for k, v in row.items():
                value = ''
                if type(v) is datetime.datetime:
                    value = v.strftime("%Y-%m-%d %H:%M:%S")
                elif v is not None:
                    value = str(v)
                shipment[k] = value
            shipments.append(shipment)
        for shipment in shipments:
            shipmentId = shipment['DocEntry']
            shipment['items'] = self._getShipmentItems(shipmentId, itemColumns)
        return shipments
