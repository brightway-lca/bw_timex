import warnings
from copy import deepcopy
from itertools import permutations


class SetList:
    """A helper class for the mapping of the same/mapped activity in different (temporal) databases, composed of a list of sets, that hold can hold the set of tuples of (act_id, database).
    It is built by adding sets to the list, and returns the matching sets if one
    calls the an item from a set.

    Example: If the class instance is called my_setlist, my_setlist.add(set).
    """

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

        Parameters
        -----
        new_set: a set to add to the SetList instance

        Returns
        -------
        None

        """

        if new_set not in self.list:
            checklist_items = [
                item for itemset in self.list for item in new_set if item in itemset
            ]
            checklist_sets = [
                itemset for itemset in self.list for item in new_set if item in itemset
            ]
            if len(checklist_items) != 0:
                warnings.warn(
                    f"tried to add {new_set} to the SetList\n, but {checklist_items} already exist in the SetList in:\n {checklist_sets}. \n Skipping {new_set}"
                )
                pass
            else:
                self.list.append(new_set)
        else:
            pass

    def __getitem__(self, key: any) -> set:
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
        if len(sets) > 1:
            warnings.warn(
                f"Key found in multiple sets! Please check {sets} ! Returning only the first set"
            )
        if len(sets) > 0:
            return sets[0]
        else:
            warnings.warn(f"Key {key} not found in SetList")
            return None

    def __len__(
        self,
    ) -> int:
        return len(self.list)

    def __repr__(self):
        return f"SetList({self.list})"


class TimeMappingDict(dict):
    """
    A dictionary mapping (flow, timestamp) tuples to unique integer IDs.
    """

    def __init__(self, start_id=2, *args, **kwargs) -> None:
        """Initializes the dictionary with a starting ID."""
        super().__init__(*args, **kwargs)
        self._current_id = start_id
        self._used_ids = set(self.values())  # Track assigned IDs efficiently
        self._modified = False  # Track changes
        self._reversed_dict = None  # Store reversed dict when needed

    def add(self, process_time_tuple, unique_id=None):
        """
        Adds a new process_time_tuple to the dictionary.

        Parameters
        ----------
        process_time_tuple : tuple
            A tuple of (flow and timestamp)
        unique_id : int, optional
            A unique ID for the tuple (default: None).

        Returns
        -------
        int
            The assigned unique ID.
        """
        # If already exists, return immediately
        if process_time_tuple in self:
            return self[process_time_tuple]

        if unique_id is not None:
            if unique_id in self._used_ids:
                raise ValueError(f"Unique ID {unique_id} is already assigned.")
            self._used_ids.add(unique_id)
            self[process_time_tuple] = unique_id
        else:
            new_id = self._current_id
            self._current_id += 1
            self[process_time_tuple] = new_id
            self._used_ids.add(new_id)

        self._modified = True  # Mark as modified
        return self[process_time_tuple]

    @property
    def reversed(self):
        """
        Returns a reversed version of the dictionary, updating it only if necessary.

        Returns
        -------
        dict
            A reversed dictionary mapping unique IDs to (flow, timestamp) tuples.
        """
        if self._modified or self._reversed_dict is None:
            self._reversed_dict = {v: k for k, v in self.items()}
            self._modified = False  # Reset modification flag
        return self._reversed_dict


class InterDatabaseMapping(dict):
    """
    A dictionary of the form {id1:{database1: id1, database2: id2, ...}, id2: ...} that maps the
    same activity in different databases.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def find_match(self, id_, db_name) -> any:
        return self[id_][db_name]

    def make_reciprocal(self):
        for mapping in list(self.values()):
            for id_ in list(mapping.values()):
                if id_ not in self.keys():
                    self[id_] = mapping
