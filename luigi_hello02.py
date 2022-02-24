# Ejecucion: definimos
#- scheduler (en este caso no necesitamos iniciar un nuevo) 
#- task a ejecutar: NameSubstituter
#- name: parameter
# python luigi_hello02.py --local-scheduler NameSubstituter --name abasharino

import luigi
import os

class HelloWorld(luigi.Task):
    def requires(self):
        return None       

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')

    def output(self):
        return luigi.LocalTarget('helloworld.txt')

class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
    
    def output(self):
        filename, file_extension = os.path.splitext(self.input().path)
        return luigi.LocalTarget(filename + '_new_' + self.name + file_extension)

if __name__ == '__main__':
    luigi.run()