"""
Tests for AllEdgeExtractor class.

This test file validates the AllEdgeExtractor class which provides an alternative
to EdgeExtractor with full graph traversal instead of priority-first approach.
"""

import pytest


class TestAllEdgeExtractorStructure:
    """Test the structure and basic properties of AllEdgeExtractor."""
    
    def test_all_edge_extractor_import(self):
        """Test that AllEdgeExtractor can be imported."""
        from bw_timex import AllEdgeExtractor
        assert AllEdgeExtractor is not None
    
    def test_all_edge_extractor_is_not_subclass_of_temporalis(self):
        """Verify that AllEdgeExtractor does NOT inherit from TemporalisLCA."""
        try:
            from bw_timex import AllEdgeExtractor
            from bw_temporalis import TemporalisLCA
            
            # AllEdgeExtractor should NOT be a subclass of TemporalisLCA
            assert not issubclass(AllEdgeExtractor, TemporalisLCA), (
                "AllEdgeExtractor should not inherit from TemporalisLCA"
            )
        except ImportError:
            pytest.skip("bw_temporalis not available")
    
    def test_all_edge_extractor_has_required_methods(self):
        """Test that AllEdgeExtractor has the required methods."""
        from bw_timex import AllEdgeExtractor
        
        # Check that the class has the required methods
        assert hasattr(AllEdgeExtractor, '__init__')
        assert hasattr(AllEdgeExtractor, 'build_edge_timeline')
        assert hasattr(AllEdgeExtractor, 'join_datetime_and_timedelta_distributions')
    
    def test_all_edge_extractor_init_signature(self):
        """Test that AllEdgeExtractor.__init__ has expected parameters."""
        from bw_timex import AllEdgeExtractor
        import inspect
        
        sig = inspect.signature(AllEdgeExtractor.__init__)
        params = list(sig.parameters.keys())
        
        # Check for required parameters
        assert 'self' in params
        assert 'lca' in params
        assert 'starting_datetime' in params
        assert 'edge_filter_function' in params
        assert 'static_activity_indices' in params
        assert 'cutoff' in params
        assert 'max_calc' in params


# Note: Full integration tests would require setting up bw2data databases,
# which is beyond the scope of this basic structural test. The actual
# functionality testing would be done in the CI environment with full dependencies.
