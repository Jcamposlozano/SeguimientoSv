import csv
from ConectorPostgres import *
from Notificaciones import *
from tqdm import tqdm
from time import sleep

class Proceso:

    def __init__(self):
        self.db_brand_buzz = ["br_tramiq","catalogos","lookertemp","pm","public","reporting","sl"]
        self.db_server1 = ["catalogos","ct","dl","dq","em","om","pm",
                        "public","reporting","seg_agencias","sl",
                        "test","treasuredata"]

    def mainProceso(self, tipoconsulta):
        self.leerBases(tipoconsulta)

    def leerBases(self,tipoconsulta):
        c = ConectorPostgres()
        n = Notificaciones()

        portConsulta = "5432"
        databaseConsulta = "db_server1"
        server1Data = []
        brandBuzzData = []

        if tipoconsulta == 'start':
            c.ejecutaConsultaBaseControl("delete from seguimiento_bases.bases_actuales")
        
        server1Data = c.consultaBases("db_server1")
        brandBuzzData = c.consultaBases("db_brand_buzz")

        totalTareas = len(server1Data) + len(brandBuzzData)
        print('INICIANDO EL PROCESO')
        contador = 0
        pbar = tqdm(total=100)
        for i in server1Data:
            contador += 1
            pbar.update(contador/totalTareas)

            # Lista todas las bases de datos en la base de datos db_server1
            if tipoconsulta == 'start':
                c.ejecutaConsultaBaseControl(
                self.insertartTable(table_catalog = databaseConsulta,
                                    table_schema = i[0],
                                    table_name = i[1],
                                    port = portConsulta))

            if tipoconsulta == 'start':
                # Determina el peso inicial antes de realizar mantenimiento
                c.ejecutaConsultaBaseControl(self.actualizaTablaInicial(
                            c.consultaPesoTablas(
                                    table_schema = i[0],
                                    table_name = i[1],
                                    port = portConsulta,
                                    database = databaseConsulta)
                                    ))

            # Mantenimiento 
            #c.ejecutaMantenimiento(database = databaseConsulta,
            #                    port = "5432",
            #                    sql = self.consultaMantenimientoanalyze(table_schema = i[0],
            #                                table_name = i[1]))

            #c.ejecutaMantenimiento(database = databaseConsulta,
            #                    port = "5432",
            #                    sql = self.consultaMantenimientofull(table_schema = i[0],
            #                                table_name = i[1]))                                            

            # Determina el peso final antes de realizar mantenimiento
            if tipoconsulta == 'end':
                c.ejecutaConsultaBaseControl(self.actualizaTablaFinal(
                            c.consultaPesoTablas(
                                    table_schema = i[0],
                                    table_name = i[1],
                                    port = portConsulta,
                                    database = databaseConsulta)))

        n.notificar("Termine", "Se termino de listar la base de datos: db_server1")
        
        ## Base db_brand_buzz

        portConsulta = "5432"
        databaseConsulta = "db_brand_buzz"

        for i in brandBuzzData:
            contador += 1
            pbar.update(contador/totalTareas)
            # Lista todas las bases de datos en la base de datos db_server1
            if tipoconsulta == 'start':
                c.ejecutaConsultaBaseControl(
                self.insertartTable(table_catalog = databaseConsulta,
                                    table_schema = i[0],
                                    table_name = i[1],
                                    port = portConsulta))

            if tipoconsulta == 'start':
                # Determina el peso inicial antes de realizar mantenimiento
                c.ejecutaConsultaBaseControl(self.actualizaTablaInicial(
                            c.consultaPesoTablas(
                                    table_schema = i[0],
                                    table_name = i[1],
                                    port = portConsulta,
                                    database = databaseConsulta)
                                    ))

            # Mantenimiento 
            #c.ejecutaMantenimiento(database = databaseConsulta,
            #                    port = "5432",
            #                    sql = self.consultaMantenimientoanalyze(table_schema = i[0],
            #                                table_name = i[1]))

            #c.ejecutaMantenimiento(database = databaseConsulta,
            #                    port = "5432",
            #                    sql = self.consultaMantenimientofull(table_schema = i[0],
            #                                table_name = i[1]))                                            

            # Determina el peso final antes de realizar mantenimiento
            if tipoconsulta == 'end':
                c.ejecutaConsultaBaseControl(self.actualizaTablaFinal(
                            c.consultaPesoTablas(
                                        table_schema = i[0],
                                        table_name = i[1],
                                        port = portConsulta,
                                        database = databaseConsulta)))

        n.notificar("Termine", "Se termino de listar la base de datos: db_brand_buzz")
                    


        pbar.close()




    def insertartTable(self, **kwargs):
        query = str("INSERT INTO seguimiento_bases.bases_actuales "
        "(table_catalog, table_schema, table_name, port) VALUES ")
        query += "('{}','{}','{}','{}');".format (kwargs['table_catalog']
                                    ,kwargs['table_schema']
                                    ,kwargs['table_name']
                                    ,kwargs['port'])
        return query

#peso_inicial
    def actualizaTablaInicial(self, respuesta):
        query = str("update seguimiento_bases.bases_actuales set " 
                    "peso_inicial = '{}' "
                    "where "
                    "table_catalog = '{}' "
                    "and table_schema = '{}' " 
                    "and table_name = '{}' "
                    "and port = '{}';").format (respuesta[4],
                                        respuesta[1],
                                        respuesta[2],
                                        respuesta[3],
                                        respuesta[0])
        return query

    def actualizaTablaFinal(self, respuesta):
        query = str("update seguimiento_bases.bases_actuales set " 
                    "peso_final = '{}' "
                    "where "
                    "table_catalog = '{}' "
                    "and table_schema = '{}' " 
                    "and table_name = '{}' "
                    "and port = '{}';").format (respuesta[4],
                                        respuesta[1],
                                        respuesta[2],
                                        respuesta[3],
                                        respuesta[0])
        return query



    def consultaMantenimientoanalyze(self, **kwargs):
        query = str(
            "VACUUM analyze \"{}\".\"{}\";").format (
                kwargs['table_schema'],kwargs['table_name'])
        return query


    def consultaMantenimientofull(self, **kwargs):
        query = str(
            "VACUUM full \"{}\".\"{}\";").format (
                kwargs['table_schema'],kwargs['table_name'])         
        return query        
