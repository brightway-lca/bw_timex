import warnings

class SetList:
    """A little helper class fot the mapping of the same/mapped activity in different (temporal) databases.
    It contains a list of sets, that hold can hold the set of tuples of (act_id, database). It is built by
    adding sets to the list, and returns the matching sets if one calls the an item from a set. 

    Example: If the class instance is called my_setlist, my_setlist.add(set). When called """

    def __init__(
        self,
    ) -> None:
        self.list = []

    def add(
        self,
        new_set: set,
    ) -> None:
        """
        This method adds a set to the SetList instance.
        Input
        -----
        new_set: a set to add to the SetList instance
        
        Returns
        -------
        None (or rather nothing)
        
        """

        if new_set not in self.list:
            checklist_items = [item for itemset in self.list for item in new_set if item in itemset]
            checklist_sets = [itemset for itemset in self.list for item in new_set if item in itemset]
            if len(checklist_items) != 0:
                warnings.warn('tried to add {} to the SetList\n, but {} already exist in the SetList in:\n {}. \n Skipping {}'.format(
                    new_set, checklist_items, checklist_sets, new_set))
                pass
            else:
                self.list.append(new_set)
        else:
            pass
    
    def __getitem__(
        self,
        key: any
    ) -> set:
        """
        Returns all sets in the SetList instance containing the key
        Inputs
        ------
        key: the key to look for in the sets of the SetList
        
        Returns
        -------
        A list containing the set or all sets 
        """
        sets = [matching_set for matching_set in self.list if key in matching_set]
        if len(sets)>1:
            warnings.warn('Key found in multiple sets!!!! {} Please check! Returning only the first set'.format(sets))
        if len(sets)>0: 
            return sets[0]
        else: 
            warnings.warn('Key {} not found in SetList'.format(key))
            return None
    
    def __len__(
        self,
    ) -> int:
        return len(self.list)


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
