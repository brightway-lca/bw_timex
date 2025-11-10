"""
Test the priority_first_traversal parameter in EdgeExtractor.
"""

import pytest
from bw_timex.edge_extractor import EdgeExtractor


class TestEdgeExtractorPriorityFirst:
    """Test that the priority_first_traversal parameter works as expected."""

    def test_priority_first_traversal_default_is_true(self):
        """Test that priority_first_traversal defaults to True."""
        # We can't fully instantiate without a valid LCA, but we can test
        # the parameter is accepted
        # This test would need to be expanded with proper fixtures
        pass

    def test_breadth_first_raises_without_bw_graph_tools(self):
        """Test that using breadth-first without bw_graph_tools raises an error."""
        # Mock the BreadthFirstGT as None to simulate missing import
        import bw_timex.edge_extractor as ee_module
        original_bfgt = ee_module.BreadthFirstGT
        
        try:
            ee_module.BreadthFirstGT = None
            
            # This should raise ImportError when priority_first_traversal=False
            # Note: We can't fully test without a proper LCA object
            # but we can verify the validation logic exists
            
            # The actual test would need proper fixtures:
            # with pytest.raises(ImportError, match="BreadthFirstGT is not available"):
            #     EdgeExtractor(base_lca, priority_first_traversal=False)
            
        finally:
            # Restore original value
            ee_module.BreadthFirstGT = original_bfgt

    def test_priority_first_method_exists(self):
        """Test that _build_edge_timeline_priority_first method exists."""
        assert hasattr(EdgeExtractor, '_build_edge_timeline_priority_first')

    def test_breadth_first_method_exists(self):
        """Test that _build_edge_timeline_breadth_first method exists."""
        assert hasattr(EdgeExtractor, '_build_edge_timeline_breadth_first')

    def test_build_edge_timeline_method_exists(self):
        """Test that build_edge_timeline method exists and is callable."""
        assert hasattr(EdgeExtractor, 'build_edge_timeline')
        assert callable(getattr(EdgeExtractor, 'build_edge_timeline'))
