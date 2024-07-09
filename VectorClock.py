from copy import deepcopy
class VectorClock:
    def __init__(self):
        self.clock={}

    def update(self,node,value):
        self.clock[node]=value

    def __lt__(self, other):
        for key in self.clock:
            if key not in other.clock:
                return False
            if self.clock[key]>other.clock[key]:
                return False
        return True

    def __le__(self, other):
        return (self==other) or (self<other)

    def __eq__(self, other):
        return self.clock==other.clock

    def __gt__(self, other):
        return other<self

    def __ge__(self, other):
        return (self==other) or (self<other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def combine(self,clocks):
        results=[]
        for clock in clocks:
            consumed=False
            for index,result in enumerate(results):
                if clock<=result:
                    consumed=True
                    break
                if result < clock:
                    results[index]=deepcopy(clock)
                    consumed=True
                    break
            if not consumed:
                results.append(deepcopy(clock))
        return results

    def converge(self,clocks):
        result = {}
        for vector in clocks:
            for key in vector.clock.keys():
                if key in result:
                    if result[key] < vector.clock[key]:
                        result[key]=vector.clock[key]
                else:
                    result[key]=vector.clock[key]
        new_clock = VectorClock()
        new_clock.clock=result
        return new_clock

    def __str__(self):
        return str(self.clock)

    def __hash__(self):
        return hash(str(self))
