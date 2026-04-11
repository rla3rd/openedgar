import os
import sys
import typer
from pathlib import Path

# Add sec_openedgar to path
sys.path.append(os.path.join(os.getcwd(), 'sec_openedgar'))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
import django
django.setup()

from openedgar.tasks import process_filings, sync_security_master
from openedgar.processes.rag_pipeline import ModernRAGPipeline
from openedgar.processes.analyst_report import AnalystReporter

app = typer.Typer(help="OpenEDGAR Analyst CLI - Direct interface for data & ML research.")

@app.command()
def ingest(year: int, qtr: int = None, form_type: str = "4"):
    """
    Ingest filings for a specific period and form type.
    """
    typer.echo(f"Starting ingestion for {year} {'Q' + str(qtr) if qtr else 'Full Year'} (Form {form_type})")
    # This runs the celery task synchronously for simplicity in the CLI
    process_filings(year=year, qtr=qtr, formtypes=[form_type])
    typer.echo("Ingestion complete.")

@app.command()
def sync():
    """
    Sync the Security Master with official SEC Ticker mapping.
    """
    typer.echo("Syncing Security Master with SEC Ticker map...")
    result = sync_security_master()
    if "error" in result:
        typer.secho(f"Sync failed: {result['error']}", fg="red")
    else:
        typer.secho(f"Sync complete! Added {result['new']} new and updated {result['updated']} symbols.", fg="green", bold=True)

@app.command()
def query(text: str, cik: str = None, count: int = 5):
    """
    Run a RAG vector search across the ingested documents.
    """
    pipeline = ModernRAGPipeline()
    results = pipeline.query(text, k=count, cik=cik)
    
    if results.empty:
        typer.echo("No matching context found.")
    else:
        for i, row in results.iterrows():
            typer.secho(f"\n[Result {i+1}] {row['accession_number']} (CIK: {row['cik']})", fg="cyan", bold=True)
            typer.echo(f"Snippet: {row['content'][:500]}...")
            typer.echo("-" * 20)

@app.command()
def report(cik: str, topic: str):
    """
    Generate an Analyst Report (LaTeX/PDF) based on SEC filings for a specific topic.
    LaTeX (pdflatex) is an optional system dependency.
    """
    typer.echo(f"Generating Research Report for {cik} on topic: '{topic}'...")
    reporter = AnalystReporter()
    result = reporter.generate_report(cik, topic)
    if "Success" in result:
        typer.secho(result, fg="green", bold=True)
    else:
        typer.secho(result, fg="yellow")

@app.command()
def notebook(remote: bool = False, port: int = 8888):
    """
    Launch a Jupyter Notebook with all Django models pre-loaded.
    Use --remote to allow access from other machines.
    """
    cmd = "python manage.py shell_plus --notebook"
    if remote:
        typer.secho(f"Launching Remote Research Lab on port {port}...", fg="yellow", bold=True)
        typer.echo("Security Token will be generated below. Use it to log in from your local browser.")
        # Bind to 0.0.0.0 and disable local browser launch
        cmd += f" -- --ip 0.0.0.0 --port {port} --no-browser --AllowRoot=True"
    else:
        typer.echo(f"Launching Local Research Lab on port {port}...")
        cmd += f" -- --port {port}"
        
    os.system(cmd)

@app.command()
def run_script(script_path: str):
    """
    Run a custom analyst script within the OpenEDGAR environment.
    """
    if not os.path.exists(script_path):
        typer.secho(f"Script not found: {script_path}", fg="red")
        raise typer.Exit()
        
    typer.echo(f"Executing analyst script: {script_path}")
    # Executes script with access to Django ORM
    os.system(f"python manage.py runscript {script_path}")

if __name__ == "__main__":
    app()
