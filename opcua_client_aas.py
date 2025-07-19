import asyncio
import logging

from asyncua import Server, ua
import time
import sys
sys.path.insert(0, "..")
import sqlite3
from opcua import ua
from opcua import Client


if __name__ == "__main__":
    
    #client = Client("opc.tcp://192.168.43.129:51210/UA/SampleServer")
    client = Client("opc.tcp://192.168.1.209:51210/freeopcua/server")
    
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user
    try:
        client.connect()
        while True:
        # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
        #No se puede convertir en float directamente la variable que se obtiene del server ya que le quita los atributos para escribir
            time.sleep(2)
            root = client.get_root_node()

                        
        #Nodes Information and variable creation    
            var_date                  = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.Date.Value")
            var_time                  = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.Time.Value")
            var_PF_average            = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.PFAverage.Value")
            var_Total_int_act_power   = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.TotalIntegralActivePower.Value")
            var_Total_int_react_power = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.TotalIntegralReactivePower.Value")
            var_Current_1             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.CurrentMeasure1.Value")
            var_Current_2             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.CurrentMeasure2.Value")
            var_Current_3             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.CurrentMeasure3.Value")
            var_Frequency_1           = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.FrequencyMeasure1.Value")
            var_Frequency_2           = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.FrequencyMeasure2.Value")
            var_Frequency_3           = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.FrequencyMeasure3.Value")
            var_Voltage_1             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.VoltageMeasure1.Value")
            var_Voltage_2             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.VoltageMeasure2.Value")
            var_Voltage_3             = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.VoltageMeasure3.Value")
            var_Total_inst_act_Power  = client.get_node("ns=3;s=AASROOT.PowerMonitor_51A.OperationalData.TotalInstantaneousActivePower.Value")
            
        #conectar a la BD
            conn = sqlite3.connect('lecturas.db')
            cursor = conn.cursor()
            
        # Obtener el último registro (con el id más alto)
            cursor.execute("SELECT * FROM lecturas ORDER BY id DESC LIMIT 1")
            ultimo_registro = cursor.fetchone()

        # Cerrar la conexión
            conn.close()

        # Verificar si se obtuvo un registro y extraer las columnas deseadas
            if ultimo_registro:
                Fecha                 = str(ultimo_registro[1])
                hora                  = str(ultimo_registro[2])
                PF_average            = float(ultimo_registro[3])
                Total_int_act_power   = float(ultimo_registro[4]) 
                Total_int_react_power = float(ultimo_registro[5]) 
                Current_1             = float(ultimo_registro[6]) 
                Current_2             = float(ultimo_registro[7]) 
                Current_3             = float(ultimo_registro[8])
                Frequency_1           = float(ultimo_registro[9]) 
                Frequency_2           = float(ultimo_registro[10]) 
                Frequency_3           = float(ultimo_registro[11]) 
                Voltage_1             = float(ultimo_registro[12]) 
                Voltage_2             = float(ultimo_registro[13]) 
                Voltage_3             = float(ultimo_registro[14]) 
                Total_inst_act_Power  = float(ultimo_registro[15])
                print("ok")

            else:
                print("No se encontraron registros en la base de datos.")

                # Server Variables Writing
            var_date.set_value(Fecha)
            var_time.set_value(hora)
            var_PF_average.set_value(ua.Variant(round(PF_average,2), ua.VariantType.Float))            
            var_Total_int_act_power.set_value(ua.Variant(Total_int_act_power, ua.VariantType.Float))   
            var_Total_int_react_power.set_value(ua.Variant(Total_int_react_power, ua.VariantType.Float)) 
            var_Current_1.set_value(ua.Variant(Current_1, ua.VariantType.Float))             
            var_Current_2.set_value(ua.Variant(Current_2, ua.VariantType.Float))             
            var_Current_3.set_value(ua.Variant(Current_3, ua.VariantType.Float))             
            var_Frequency_1.set_value(ua.Variant(Frequency_1, ua.VariantType.Float))           
            var_Frequency_2.set_value(ua.Variant(Frequency_2, ua.VariantType.Float))           
            var_Frequency_3.set_value(ua.Variant(Frequency_3, ua.VariantType.Float))           
            var_Voltage_1.set_value(ua.Variant(Voltage_1, ua.VariantType.Float))             
            var_Voltage_2.set_value(ua.Variant(Voltage_2, ua.VariantType.Float))             
            var_Voltage_3.set_value(ua.Variant(Voltage_3, ua.VariantType.Float))             
            var_Total_inst_act_Power.set_value(ua.Variant(Total_inst_act_Power, ua.VariantType.Float))
            print("ok")  


    finally:
        client.disconnect()

