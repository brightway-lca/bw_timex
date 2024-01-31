class TimeMappingDict(dict):
    def __init__(self, start_id=2, *args, **kwargs): # start_id doesnt work if set lower than 2. WHY?!?!
        super().__init__(*args, **kwargs)
        self._current_id = start_id

    def add(self, process_time_tuple):
        if process_time_tuple not in self:
            self[process_time_tuple] = self._current_id
            self._current_id += 1
