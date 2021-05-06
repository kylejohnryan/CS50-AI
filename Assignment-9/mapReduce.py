from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class Pair2Support(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper_getPairs,
            reducer = self.reducer_countPairs)
        ]

    def mapper_getPairs(self, _, line):
        words = line.split(',')
        for i, word in enumerate(words):
            if (i+1) < len(words):
                yield (word, words[i + 1]), 1

    def reducer_countPairs(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    Pair2Support.run()