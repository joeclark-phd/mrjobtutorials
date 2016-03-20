from mrjob.job import MRJob

# simple one-step map-reduce pipeline to calculate the total 
# count of births grouped by the first letter of the name

class MRNamesByLetter(MRJob):

  def mapper(self, key, record):
    splits = record.split(",")
    yield splits[0][0], splits[2]
    # output: first letter of name, # of births
    
  def reducer(self, letter, births):
    # input: first letter of name, list(# of births)
    yield letter, sum(births)

if __name__ == '__main__':
    MRNamesByLetter.run()