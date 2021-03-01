import psycopg2

class ConectorPostgres:

    def __init__(self):
        self.user = 'app_knime'
        self.password = 'knime2020#'
        self.host = '10.170.3.3'
        self.port = '5432'
        self.Tablas = []
        self.TablasSize = []
        self.solicitarVaruiables()


    def solicitarVaruiables(self):      
        print('PARAMETRIA PARA CONECTAR AL SERVIDOR')  
        cambio = input('Desea cambiar variables (N/y): ').upper() 
        if cambio == 'Y':
            host1 = input('HOST (10.170.3.3): ')
            port1 = input('PORT (5432): ')
            user1 = input('USER (app_knime): ')
            password1 = input('PASSWORD (knime2020#): ')

            if host1 != '':
                self.host = host1
            if port1 != '':
                self.port = port1
            if user1 != '':
                self.user = user1
            if password1 != '':
                self.password = password1
        print('***********************************')


    def consultaBases(self, databaseE:str ):
        registro = []
        self.Tablas = []
        c = psycopg2.connect(database = databaseE, user = self.user,password = self.password, host = self.host ,port = self.port)
        try:
            with c.cursor() as cursor:
                sql = str("select table_schema,table_name from information_schema.tables where table_type = 'BASE TABLE' and "
                        "table_schema not in ('pg_catalog','information_schema') order by 1,2;")
                cursor.execute(sql)
                for (table_schema,table_name) in cursor:
                    registro = []
                    registro.append(table_schema)
                    registro.append(table_name)
                    self.Tablas.append(registro)
                cursor.close()
                c.close()
            return self.Tablas
        finally:
            pass        

    def ejecutaConsultaBaseControl(self, sql):
        try:
            con = psycopg2.connect(database = 'db_marketingWeb', user = self.user,password = self.password, host = self.host ,port = '5433')                
            cur = con.cursor()
            cur.execute(str(sql))
            con.commit()
            con.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def ejecutaConsulta(self, sql):
        try:
            con = psycopg2.connect(database = self.database, user = self.user,password = self.password, host = self.host ,port = self.port)                
            cur = con.cursor()
            cur.execute(str(sql))
            con.commit()
            con.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def ejecutaMantenimiento(self, **kwargs):
        try:
            con = psycopg2.connect(database = kwargs["database"], user = self.user,password = self.password, host = self.host , port = kwargs["port"])                
            cur = con.cursor()
            print(str(kwargs["sql"]))
            cur.execute(str(kwargs["sql"]))
            con.commit()
            con.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def consultaPesoTablas(self, **kwargs ):
        self.TablasSize = []
        #sql = str("SELECT pg_size_pretty(pg_total_relation_size(\"{}\".\"{}\"));".format(kwargs["table_schema"],kwargs["table_name"] ))
        #print(sql)
        c = psycopg2.connect(database = kwargs["database"], user = self.user,password = self.password, host = self.host , port = kwargs["port"])
        try:
            with c.cursor() as cursor:
                sql = str("SELECT pg_size_pretty(pg_total_relation_size('\"{}\".\"{}\"'));".format(kwargs["table_schema"],kwargs["table_name"] ))
                cursor.execute(sql)
                for (pg_size_pretty) in cursor:
                    self.TablasSize.append(kwargs["port"])
                    self.TablasSize.append(kwargs["database"])
                    self.TablasSize.append(kwargs["table_schema"])
                    self.TablasSize.append(kwargs["table_name"])
                    self.TablasSize.append(pg_size_pretty[0])
                cursor.close()
                c.close()
            return self.TablasSize
        finally:
            pass  
        