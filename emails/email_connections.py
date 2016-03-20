from mrjob.job import MRJob
from mrjob.step import MRStep

# map-reduce pipeline to take a dataset of e-mails and find out
# who are the top 3 people emailing any given person (i.e. Hillary Clinton)

class MRTopSenders(MRJob):

  def mapper(self, key, record):
    # This input file is very messy; most lines are not CSV data,
    # therefore let's first ignore invalid lines. Valid lines
    # should have 22 comma-separated fields. Let's check for >20
    splits = record.split(",")
    if len(splits) > 20:
      # also skip lines where either "to" or "from" is blank
      if len(splits[3]) and len(splits[4]):
        yield (splits[3],splits[4]), 1
        # emits a (to,from) tuple as key, with 1 as value

  def sum_by_from(self, tofrom, count):
    yield tofrom[0], (sum(count),tofrom[1])
    # output is "to",(count,"from")
    # This will allow the next step to sort the senders by number sent
    
  def find_top_three(self, to, counts):
    topsenders = sorted(counts)[0:3]
    fromnames = [ ts[1] for ts in topsenders ]
    yield to, ",".join(fromnames)
    # output is "to","from1,from2,from3"

  def hillary_only(self,to,topsenders):
    if to == "H":
      yield to, topsenders

      
  def steps(self):
    return [ MRStep(mapper=self.mapper,reducer=self.sum_by_from),
             MRStep(reducer=self.find_top_three),
             MRStep(mapper=self.hillary_only) ]

      
if __name__ == '__main__':
    MRTopSenders.run()