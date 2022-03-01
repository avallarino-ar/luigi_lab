import luigi
import logging
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests as rq


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


class downloadData(luigi.Task):
    downloaddate = luigi.Parameter()
    productoid = luigi.Parameter()

    def requires(self):
        return None

    def run(self):
        url = 'http://www.economia-sniim.gob.mx/Nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/'+\
              'ResultadosConsultaFechaFrutasYHortalizas.aspx?'+\
              'fechaInicio='+ self.downloaddate +\
              '&fechaFinal='+ self.downloaddate +\
              '&ProductoId='+ self.productoid +\
              '&OrigenId=-1&Origen=Todos&DestinoId=-1&Destino=Todos&PreciosPorId=2&RegistrosPorPagina=500'

        logger.info(" === downloadData/run... === ")
        # Extract HTMl tree
        page = rq.get(url).text
        soup = bs(page)

        # Find data table:
        table = soup.find('table', id='tblResultados')
        table_rows = table.find_all('tr')

        res = []
        for tr in table_rows[3:]:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            res.append(row)

        df_data = pd.DataFrame(res, columns=["presentacion", "origen", "destino", "precio_min", "precio_max", "precio_frec", "obs"])
        df_data["date"] = self.downloaddate
        df_data["producto_id"] = self.productoid

        f = self.output().open('w')
        df_data.to_csv(f, sep=',', encoding='utf-8', index=None)
        f.close()

    def output(self):
        logger.info(" === downloadData/output... === ")
        return luigi.LocalTarget(path="data/01_raw_data.csv", format=luigi.format.Nop)



class processData(luigi.Task):
    downloaddate = luigi.Parameter()
    productoid = luigi.Parameter()

    def requires(self):
        logger.info(" === processData/requires... === ")
        return downloadData(self.downloaddate, self.productoid)

    def run(self):
        logger.info(" === processData/run... === ")
        df_data = pd.read_csv("data/01_raw_data.csv")
        df_data.drop(columns=['obs'], inplace=True)
        df_data['precio_medio'] =  (df_data['precio_min'] +  df_data['precio_max']) / 2

        f = self.output().open('w')
        df_data.to_csv(f, sep=',', encoding='utf-8', index=None)
        f.close()

    def output(self):
        logger.info(" === processData/output... === ")
        return luigi.LocalTarget(path="data/02_process_data.csv", format=luigi.format.Nop)



class summaryData(luigi.Task):
    downloaddate = luigi.Parameter()
    productoid = luigi.Parameter()

    def requires(self):
        logger.info(" === summaryData/requires... === ")
        return processData(self.downloaddate, self.productoid)

    def run(self):
        logger.info(" === summaryData/run... === ")
        df_data = pd.read_csv("data/02_process_data.csv")

        f = self.output().open('w')
        df_data.describe().applymap("{0:.2f}".format).to_csv(f, sep=',', encoding='utf-8', index=None)
        f.close()


    def output(self):
        logger.info(" === summaryData/output... === ")
        return luigi.LocalTarget(path="data/03_summary_data.csv", format=luigi.format.Nop)


if __name__ == '__main__':
    luigi.run()


"""
http://www.economia-sniim.gob.mx/Nuevo/Home.aspx?opcion=Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ConsultaFrutasYHortalizas.aspx?SubOpcion=4%7C0

python pipeline_02.py --local-scheduler downloadData --downloaddate  04/01/2022 --productoid 133

python pipeline_02.py --scheduler-host localhost processData --downloaddate  04/01/2022 --productoid 133
    
python pipeline_02.py --scheduler-host localhost summaryData --downloaddate  04/01/2022 --productoid 133 
"""