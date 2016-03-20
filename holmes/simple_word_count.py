from mrjob.job import MRJob



class MRWordCount(MRJob):

    def mapper(self, _, line):
        # yield each word in the line
        words = line.split()
        for word in words:
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)


if __name__ == '__main__':
    MRWordCount.run()