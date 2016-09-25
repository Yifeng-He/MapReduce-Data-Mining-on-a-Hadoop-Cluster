from mrjob.job import MRJob
from mrjob.step import MRStep

class DelayedFlights(MRJob):
    
    # define two steps
    def steps(self):
        step1 = MRStep(mapper = self.mapper_step1, reducer = self.reducer_step1)
        step2 = MRStep(mapper = self.mapper_step2, reducer = self.reducer_step2)
        return [step1, step2]
    
    # define a fundtion to find the top-k delays from a list of delays
    def findTopKDelays(self, list_pairs, k):
        # sort the list based on delay in descending order
        sorted_list_pairs = sorted(list_pairs, reverse = True)
        m = min(len(sorted_list_pairs), k)
        return sorted_list_pairs[:m]
        
    # defien step-1 mapper function
    def mapper_step1(self, key, line):
        # extract flight number and departure delay from the input data
        fields = line.split(',')
        # the departure delay
        departure_delay = fields[15]
        if departure_delay.isdigit():
            flight_number = int(fields[9])
            departure_delay = float(departure_delay)
            yield (flight_number, departure_delay)
        
    # define step-1 reducer function
    def reducer_step1(self, flight_number, departure_delays):
        # convert to a list
        list_delays = list(departure_delays)
        # calculate the average delay
        avg_delay = sum(list_delays)/len(list_delays)
        yield (flight_number, avg_delay)
        
    # define step-2 mapper function
    def mapper_step2(self, flight_number, avg_delay):
        yield (1, (avg_delay, flight_number))
        
    # define step-2 reducer function
    def reducer_step2(self, key, delay_flight_delay_pairs):
        # convert to a list
        list_pairs = list(delay_flight_delay_pairs)
        k = 10
        list_topK_delays = self.findTopKDelays(list_pairs, k)
        for item in list_topK_delays:
            yield (item[1], item[0]) #(flight_number, delay)

# main program
if __name__ == '__main__':
    DelayedFlights.run()
