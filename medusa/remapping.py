class TimeMappingDict(dict):
    def __init__(self, start_id=2, *args, **kwargs): # start_id doesnt work if set lower than 2. WHY?!?!
        super().__init__(*args, **kwargs)
        self._current_id = start_id
        self._check_id = start_id - 1  # check_id that is different from the start id for the reversed dict

    def add(self, process_time_tuple):
        if process_time_tuple not in self:
            self[process_time_tuple] = self._current_id
            self._current_id += 1

    def reversed(self):
        '''return a reversed version of dict, update if necessary'''
        if self._check_id != self._current_id:
            self.reversed_dict = {v:k for k,v in self.items()}
            self._check_id = self._current_id
        return self.reversed_dict

