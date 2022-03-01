import luigi
import requests
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


class downloadData(luigi.Task):
    fileurl = luigi.Parameter()
    filedata = luigi.Parameter()

    def requires(self):
        return None

    def run(self):
        logger.info(" === downloadData/run... === ")
        data = requests.get(self.fileurl)
        with self.output().open('wb') as outfile:
            outfile.write(data.content)

    def output(self):
        logger.info(" === downloadData/output... === ")
        return luigi.LocalTarget(path="data/01_raw_"+self.filedata, format=luigi.format.Nop)



class processData(luigi.Task):
    fileurl = luigi.Parameter()
    filedata = luigi.Parameter()

    def requires(self):
        logger.info(" === processData/requires... === ")
        return downloadData(self.fileurl, self.filedata)

    def run(self):
        logger.info(" === processData/run... === ")
        df_data = pd.read_csv("data/01_raw_"+self.filedata)
        df_data.columns = [col.replace(".", "_") for col in df_data.columns]

        f = self.output().open('w')
        df_data.to_csv(f, sep=',', encoding='utf-8', index=None)
        f.close()

    def output(self):
        logger.info(" === processData/output... === ")
        return luigi.LocalTarget(path="data/02_process_" + self.filedata, format=luigi.format.Nop)


class summaryData(luigi.Task):
    fileurl = luigi.Parameter()
    filedata = luigi.Parameter()

    def requires(self):
        logger.info(" === summaryData/requires... === ")
        return processData(self.fileurl, self.filedata)

    def run(self):
        logger.info(" === summaryData/run... === ")
        df_data = pd.read_csv("data/02_process_"+self.filedata)

        f = self.output().open('w')
        df_data.describe().applymap("{0:.2f}".format).to_csv(f, sep=',', encoding='utf-8', index=None)
        f.close()


    def output(self):
        logger.info(" === summaryData/output... === ")
        return luigi.LocalTarget(path="data/03_summary_" + self.filedata, format=luigi.format.Nop)


if __name__ == '__main__':
    luigi.run()


"""
python pipeline_02.py --local-scheduler downloadData \
    --fileurl https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv \
    --filedata iris.csv


python pipeline_02.py --scheduler-host localhost processData \
    --fileurl https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv \
    --filedata iris.csv
    
    
python pipeline_02.py --scheduler-host localhost summaryData \
    --fileurl https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv \
    --filedata iris.csv    
"""