#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from prefect import flow, task
import papermill as pm
import subprocess
import psycopg2

@task
def run_notebook(notebook_path, output_path):
    pm.execute_notebook(notebook_path, output_path)

@task
def run_sql_script(database, user, password, host, port, sql_file):
    connection = psycopg2.connect(
        dbname=DB_OFF_Viz,
        user=postgres,
        password=offviz,
        host=localhost,
        port=5432
    )
    cursor = connection.cursor()
    with open(sql_file, 'r') as file:
        sql = file.read()
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()

@task
def open_powerbi_file(pbix_path):
    subprocess.run(['C:\\Program Files\\WindowsApps\\Microsoft.MicrosoftPowerBIDesktop_2.130.930.0_x64__8wekyb3d8bbwe\\bin\PBIDesktop.exe', pbix_path])

@flow
def GLOFFViz_workflow():
    run_notebook("Anexo1_descarga_pretratamiento_dts_OFF.ipynb", "output/Anexo1_output.ipynb")
    run_notebook("Anexo2_Limpieza_parte2.ipynb", "output/Anexo2_output.ipynb")
    run_notebook("Anexo3_cargacsv_dbPostgresql.ipynb", "output/Anexo3_output.ipynb")
    run_sql_script(
        database="DB_OFF_Viz",
        user="postgres",
        password="offviz",
        host="localhost",
        port="5432",
        sql_file="Anexo4_queries_pgsql.sql"
    )
    run_notebook("Anexo5_Prediction_modRegression.ipynb", "output/Anexo5_output.ipynb")
    open_powerbi_file("path/to/OFF_VIZ.pbix")

if __name__ == "__main__":
    data_processing_and_analysis_workflow()

