from django.shortcuts import render
from django.views.generic import View
from django.http import JsonResponse
import json
import logging
from .processes.rag_pipeline import RAGPipeline

logger = logging.getLogger(__name__)

class RAGChatView(View):
    """
    Handles the Premium RAG Dashboard.
    GET: Returns the dashboard template.
    POST: Processes a search/chat query and returns semantic fragments + LLM response.
    """
    def get(self, request, *args, **kwargs):
        return render(request, 'pages/rag_dashboard.html')

    async def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            query = data.get('query', '')
            
            if not query:
                return JsonResponse({'error': 'No query provided'}, status=400)
            
            pipeline = RAGPipeline()
            
            # Step 1: Perform RAG search
            results_df = pipeline.query_rag(query)
            
            if results_df is None or results_df.empty:
                return JsonResponse({
                    'response': "I couldn't find any relevant filings for that query in the database.",
                    'fragments': []
                })
            
            # Step 2: Prepare fragments for frontend
            fragments = results_df.to_dict(orient='records')
            
            # Step 3: Query Local LLM (Qwen 3.5 via LM Studio)
            context = "\n\n".join([f"[{f['form_type']} {f['date_filed']}]: {f['content']}" for f in fragments])
            prompt = f"Use the following SEC filing fragments to answer the user query.\n\nContext:\n{context}\n\nQuery: {query}"
            
            llm_response = await pipeline.query_local_llm(prompt)
            
            return JsonResponse({
                'response': llm_response,
                'fragments': fragments
            })
            
        except Exception as e:
            logger.exception("Error in RAGChatView")
            return JsonResponse({'error': str(e)}, status=500)
