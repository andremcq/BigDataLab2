#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_forest_fires_rain_area,
                   reducer=self.reducer_get_avg_forest_fires_rain_area)
        ]

    def mapper_get_forest_fires_rain_area(self, _, line):
        # yield each burnt land area and whether or not it rained
        data = DATA_RE.findall(line)
        area = float(data[12])
        if (float(data[11])>0):# Rain check
            yield ("Rain", area)
        else:
            yield ("No rain", area)

    def reducer_get_avg_forest_fires_rain_area(self, key, values):
        # get avg forest fires rain\no rain burnt area
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield (key + " : avg area burnt in hectares", round(total,1) / size)

if __name__ == '__main__':
    MRProb2_3.run()
