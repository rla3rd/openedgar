import os
import subprocess
from string import Template
from openedgar.processes.rag_pipeline import ModernRAGPipeline
from openedgar.models import Company

LATEX_TEMPLATE = """\\documentclass[12pt, a4paper]{article}
\\usepackage[utf8]{inputenc}
\\usepackage{geometry}
\\usepackage{hyperref}
\\usepackage{charter} % Premium font
\\geometry{margin=1in}

\\title{\\textbf{Analyst Report: $company_name}}
\\author{OpenEDGAR Research Platform}
\\date{\\today}

\\begin{document}

\\maketitle

\\begin{abstract}
This report was generated using RAG-assisted analysis of SEC filings regarding the topic: \\textit{$topic}.
\\end{abstract}

\\section*{Executive Summary}
$summary

\\section*{Key Findings (SEC Fragments)}
$fragments

\\end{document}
"""

class AnalystReporter:
    def __init__(self, output_dir="reports"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.pipeline = ModernRAGPipeline()
        
    def generate_report(self, cik: str, topic: str) -> str:
        """Generates a LaTeX report and attempts to compile it to PDF."""
        # 1. Fetch Company details
        try:
            company = Company.objects.get(cik=cik)
            company_name = f"{company.cik_name} ({company.ticker or cik})"
        except Company.DoesNotExist:
            company_name = f"Company CIK: {cik}"
            
        # 2. RAG Query
        results = self.pipeline.query(topic, cik=cik, k=3)
        
        # 3. Format Fragments
        import asyncio
        if results.empty:
            summary = "No relevant SEC filings found for this topic."
            fragments_ltx = "None."
        else:
            fragments = results.to_dict(orient='records')
            context = "\\n\\n".join([f"[{f['form_type']} {f['date_filed']}]: {f['content']}" for f in fragments])
            
            # Simple summarization prompt
            prompt = f"Summarize the following SEC findings concisely for an executive summary:\\n\\n{context}"
            
            try:
                loop = asyncio.get_event_loop()
                summary = loop.run_until_complete(self.pipeline.query_local_llm(prompt))
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("LLM Summary failed, using raw extraction.", exc_info=e)
                summary = "LLM Summary unavailable. Please review the fragments below."
                
            fragments_ltx = ""
            for i, f in enumerate(fragments):
                clean_content = f['content'].replace('&', '\\&').replace('%', '\\%').replace('$', '\\$').replace('_', '\\_')
                fragments_ltx += f"\\subsection*{{Result {i+1}: {f['form_type']} ({f['date_filed']})}}\\n"
                fragments_ltx += f"{clean_content}\\n\\n"
        
        # 4. Render LaTeX Template
        summary_clean = summary.replace('&', '\\&').replace('%', '\\%').replace('$', '\\$').replace('_', '\\_')
        tex_content = Template(LATEX_TEMPLATE).substitute(
            company_name=company_name,
            topic=topic.replace('&', '\\&'),
            summary=summary_clean,
            fragments=fragments_ltx
        )
        
        # 5. Write to Disk
        safe_name = company_name.split(" ")[0].lower().replace(",", "")
        tex_path = os.path.join(self.output_dir, f"report_{safe_name}_{cik}.tex")
        with open(tex_path, "w") as f:
            f.write(tex_content)
            
        # 6. Optional PDF Compilation
        return self._compile_pdf(tex_path)

    def _compile_pdf(self, tex_path: str) -> str:
        """Compiles to PDF if pdflatex is installed on the host."""
        try:
            # Check for pdflatex (Optional dependency)
            subprocess.run(["pdflatex", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            
            subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", "-output-directory", self.output_dir, tex_path],
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL,
                check=True
            )
            pdf_path = tex_path.replace('.tex', '.pdf')
            return f"Success! PDF generated at: {pdf_path}"
        except FileNotFoundError:
            return f"LaTeX (pdflatex) is not installed. Raw report saved at: {tex_path}. Install TexLive or process via Overleaf."
        except subprocess.CalledProcessError:
            return f"PDF Compilation failed. LaTeX syntax error likely. Raw report saved at: {tex_path}"
