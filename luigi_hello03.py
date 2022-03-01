# Visualizing running workflows (optional)
#- scheduler: --scheduler-host localhost
#- task a ejecutar: NameSubstituter
#- name: parameter
# python luigi_hello03.py --scheduler-host localhost NameSubstituter --name test

import luigi
import time
import os

class HelloWorld(luigi.Task):
    def requires(self):
        print("HelloWorld - requires")
        return None

    def run(self):
        print("HelloWorld - run")
        time.sleep(10)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
        time.sleep(10)

    def output(self):
        print("HelloWorld - output")
        return luigi.LocalTarget('helloworld.txt')


class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()
        
    def run(self):
        print("NameSubstituter - run")
        time.sleep(10)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        # sys.exit("Se ha superado el tiempo de espera. Se detendrá el pipeline.")
        time.sleep(10)     # Delay

    def output(self):
        filename, file_extension = os.path.splitext(self.input().path)
        return luigi.LocalTarget(filename + '_new_' + self.name + file_extension)


if __name__ == '__main__':
    luigi.run()

#- Start up the daemon:
# luigid

#- browser, fire up the following web address:
# http://localhost:8082    