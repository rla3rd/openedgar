import pytest
from openedgar.processes.rag_pipeline import ModernRAGPipeline
from openedgar.models import Company

@pytest.mark.django_db
def test_rag_pipeline_search(mocker):
    """
    Test the RAG search flow with a mocked HDB backend.
    """
    # Create a mock company
    Company.objects.create(cik=320193, cik_name="Apple Inc", ticker="AAPL")
    
    pipeline = ModernRAGPipeline()
    
    # Mock the HDB query result
    import pandas as pd
    mock_df = pd.DataFrame([{
        'content': 'Apple reported strong earnings.',
        'cik': '320193',
        'form_type': '10-Q',
        'date_filed': '2024-01-01',
        'distance': 0.1
    }])
    
    mocker.patch.object(pipeline.db.table, 'query', return_value=mock_df)
    
    results = pipeline.query("How is Apple doing?")
    assert not results.empty
    assert results.iloc[0]['ticker'] == 'AAPL'
