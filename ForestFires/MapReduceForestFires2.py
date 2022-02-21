#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")

""" Forest fires data
   1. X - x-axis spatial coordinate within the Montesinho park map: 1 to 9
   2. Y - y-axis spatial coordinate within the Montesinho park map: 2 to 9
   3. month - month of the year: "jan" to "dec" 
   4. day - day of the week: "mon" to "sun"
   5. FFMC - FFMC index from the FWI system: 18.7 to 96.20
   6. DMC - DMC index from the FWI system: 1.1 to 291.3 
   7. DC - DC index from the FWI system: 7.9 to 860.6 
   8. ISI - ISI index from the FWI system: 0.0 to 56.10
   9. temp - temperature in Celsius degrees: 2.2 to 33.30
   10. RH - relative humidity in %: 15.0 to 100
   11. wind - wind speed in km/h: 0.40 to 9.40 
   12. rain - outside rain in mm/m2 : 0.0 to 6.4 
   13. area - the burned area of the forest (in ha): 0.00 to 1090.84 
 """

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_forest_fires,
                   reducer=self.reducer_get_daily)
        ]

    def mapper_get_forest_fires(self, _, line):
        # yield each fire detail monthly temperature
        data = DATA_RE.findall(line)
        day = data[3]
        yield (day, 1)

    def reducer_get_daily(self, key, values):
        # get fire count per day
        total = 0
        for val in values:
            total += 1
        yield (key + " total", total)
if __name__ == '__main__':
    MRProb2_3.run()
