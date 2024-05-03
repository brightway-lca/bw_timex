class TimeMappingDict(dict):
    """
    This class is used to create a dictionary that maps a tuple of (flow and timestamp) to an unique integer id.
    """

    def __init__(self, start_id=2, *args, **kwargs) -> None:
        """"
        Initializes the `TimeMappingDict` object.

        Parameters
        ----------
        start_id : int, optional
            The starting id for the mapping. Default is 2.
        *args : Variable length argument list
        **kwargs : Arbitrary keyword arguments

        Returns
        -------
        None
        """

        super().__init__(*args, **kwargs)
        self._current_id = start_id
        self._check_id = (
            start_id - 1
        )  # check_id that is different from the start id for the reversed dict

    def add(self, process_time_tuple, unique_id=None):
        """"
        Adds a new process_time_tuple to the `TimeMappingDict` object.

        Parameters
        ----------
        process_time_tuple : tuple
            A tuple of (flow and timestamp)
        unique_id : int, optional
            An unique id for the process_time_tuple. Default is None.

        Returns
        -------
        int
            An unique id for the process_time_tuple, and adds it to the dictionary, if not already present.
        """
        if process_time_tuple in self:
            return self[process_time_tuple]

        if unique_id is not None:
            self[process_time_tuple] = unique_id
            return unique_id

        self[process_time_tuple] = self._current_id
        self._current_id += 1

        return self._current_id - 1

    def reversed(self):
        """return a reversed version of dict, update if necessary

        Parameters
        ----------
        None

        Returns
        -------
        dict
            A reversed dictionary of the `TimeMappingDict` object.
        """


        if self._check_id != self._current_id:
            self.reversed_dict = {v: k for k, v in self.items()}
            self._check_id = self._current_id
        return self.reversed_dict
