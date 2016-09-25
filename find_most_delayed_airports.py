'''
To run the code:
python find_most_delayed_airports.py --name=airports.csv 2008_small.csv
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class DelayedAirports(MRJob):
    
    def configure_options(self):
        super(DelayedAirports, self).configure_options()
        self.add_file_option('--names', help='file path to the airport names')
        
    # define two steps
    def steps(self):
        step1 = MRStep(mapper = self.mapper_step1, reducer = self.reducer_step1)
        step2 = MRStep(mapper = self.mapper_step2, reducer_init = self.reducer_init_step2, reducer = self.reducer_step2)
        return [step1, step2]
    
    # define a fundtion to find the top-k delays from a list of delays
    def findTopKDelays(self, list_pairs, k):
        # sort the list based on delay in descending order
        sorted_list_pairs = sorted(list_pairs, reverse = True)
        m = min(len(sorted_list_pairs), k)
        return sorted_list_pairs[:m]
        
    # defien step-1 mapper function
    def mapper_step1(self, key, line):
        # extract origin airport code and departure delay from the input data
        fields = line.split(',')
        # the departure delay
        departure_delay = fields[15]
        if departure_delay.isdigit():
            airport_code = fields[16]
            departure_delay = float(departure_delay)
            yield (airport_code, departure_delay)
        
    # define step-1 reducer function
    def reducer_step1(self, airport_code, departure_delays):
        # convert to a list
        list_delays = list(departure_delays)
        # calculate the average delay
        avg_delay = sum(list_delays)/len(list_delays)
        yield (airport_code, avg_delay)
        
    # define step-2 mapper function
    def mapper_step2(self, airport_code, avg_delay):
        yield (1, (avg_delay, airport_code))
    
    # define step-2 reducer_init function
    def reducer_init_step2(self):
        self.name_dictionary = {}
        with open('airports.csv') as f:
            for line in f:
                fields=line.split(',')
                # defien list_names =[code, airport, city, state, country]
                list_names = fields[:5] 
                # strip the double quotes
                list_names_cleaned = [x[1:-1] for x in list_names]
                code = list_names_cleaned[0]
                airport_full_name = ', '.join(list_names_cleaned[1:5])
                self.name_dictionary[code] = airport_full_name
             
    # define step-2 reducer function
    def reducer_step2(self, key, delay_flight_delay_pairs):
        # convert to a list
        list_pairs = list(delay_flight_delay_pairs)
        k = 10
        list_topK_delays = self.findTopKDelays(list_pairs, k)
        for item in list_topK_delays:
            yield (self.name_dictionary[item[1]], item[0]) #(airport_name, delay)

# main program
if __name__ == '__main__':
    DelayedAirports.run()
    
