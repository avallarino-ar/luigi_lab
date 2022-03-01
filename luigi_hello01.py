#- task a ejecutar: HelloWorld
# python luigi_hello01.py --local-scheduler HelloWorld

import luigi

class HelloWorld(luigi.Task):
    def requires(self):
        return None

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!!!\n')

    def output(self):
        return luigi.LocalTarget('helloworld.txt')

if __name__ == '__main__':
    luigi.run()