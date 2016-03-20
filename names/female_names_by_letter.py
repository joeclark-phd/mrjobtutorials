from mrjob.job import MRJob

# two-step map-reduce pipeline to calculate the total 
# count of female births grouped by the first letter of the name

class MRFemaleNamesByLetter(MRJob):

  def filter_by_gender(self, key, record):
    splits = record.split(",")
    if splits[1]=="F":
      yield "F", (splits[0]+","+splits[2])

  def count_by_letter(self, key, record):
    # input: ("F","name,count")
    splits = record.split(",")
    yield splits[0][0], int(splits[1])
    # output: first letter of name, # of births
    
  def sum_births(self, letter, births):
    # input: first letter of name, list(# of births)
    yield letter, sum(births)

    
  def steps(self):
  # notice this code is a little different than the previous tutorial;
  # we don't user the MRStep object, but it seems to work the same way.
    return [self.mr(mapper=self.filter_by_gender),
            self.mr(mapper=self.count_by_letter,
                    reducer=self.sum_births)]
    
    
if __name__ == '__main__':
    MRFemaleNamesByLetter.run()