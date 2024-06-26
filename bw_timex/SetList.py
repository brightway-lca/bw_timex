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
