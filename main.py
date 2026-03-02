# """
# Main Orchestrator with Factory Pattern
# File: main.py
# """

# import os
# import sys
# import json
# import datetime
# from pathlib import Path
# from dotenv import load_dotenv

# from providers.provider_factory import create_provider
# from utils.parser import extract_file_ids_from_response

# load_dotenv()

# PROJECT_ROOT = Path(__file__).parent


# class PipelineOrchestrator:
#     """Main orchestrator using Factory Pattern"""
    
#     def __init__(self, provider_id: str = 'claude'):
#         self.project_root = PROJECT_ROOT
#         self.provider_id = provider_id
#         self.step_results = {}
#         self.total_tokens = {'input': 0, 'output': 0, 'total': 0}
#         self.total_cost = 0.0
        
#         # Create provider using factory
#         self.provider = create_provider(provider_id)
        
#         # Setup output directory
#         pipeline_run_id = int(datetime.datetime.now().timestamp() * 1000)
#         self.output_dir = f"outputs/{pipeline_run_id}"
#         os.makedirs(self.output_dir, exist_ok=True)
        
#         # File tracking
#         self.uploaded_files = {}
#         self.downloaded_files = {}
#         self.uploaded_filepath = f"{self.output_dir}/uploaded_files.json"
#         self.downloaded_filepath = f"{self.output_dir}/downloaded_files.json"
        
#         # Load existing files if available
#         self._load_existing_files()
        
#         print(f"\n{'='*70}")
#         print(f"Pipeline Orchestrator Initialized")
#         print(f"{'='*70}")
#         print(f"Provider: {self.provider_id.upper()}")
#         print(f"Output Directory: {self.output_dir}")
#         print(f"{'='*70}\n")
    
#     def _load_existing_files(self):
#         """Load existing uploaded/downloaded files"""
#         if os.path.exists(self.uploaded_filepath):
#             with open(Path(self.uploaded_filepath), 'r') as f:
#                 self.uploaded_files = json.load(f)
        
#         if os.path.exists(self.downloaded_filepath):
#             with open(Path(self.downloaded_filepath), 'r') as f:
#                 self.downloaded_files = json.load(f)
    
#     def upload_files(self):
#         """Upload files using provider"""
#         print("\n" + "=" * 70)
#         print(f"UPLOAD FILES TO {self.provider_id.upper()}")
#         print("=" * 70 + "\n")
        
#         # Configuration
#         upload_files_directory = "for_code_execution_openai/files_to_upload"
#         # upload_files_directory = "for_code_execution/files_to_upload"
#         zipped_file = [
#             f"{upload_files_directory}/custom_instructions.md",
#             f"{upload_files_directory}/output_format.md",
#             f"{upload_files_directory}/project_context.md",
#             f"{upload_files_directory}/review_guidelines.md"
#         ]
        
#         try:
#             if not self.uploaded_files:
#                 # Upload files using provider
#                 results = self.provider.upload_multiple_files(zipped_file)
                
#                 # Process results
#                 successful = [r for r in results if r['success']]
#                 failed = [r for r in results if not r['success']]
#                 all_uploaded = len(failed) == 0
                
#                 # Print summary
#                 print(f"\n{'=' * 60}")
#                 print("UPLOAD SUMMARY")
#                 print(f"{'=' * 60}")
#                 print(f"\nSuccessful: {len(successful)}/{len(results)}")
                
#                 if successful:
#                     print(f"\nUploaded Files:")
#                     for result in successful:
#                         print(f"  ✓ {Path(result['file_path']).name}")
#                         print(f"    ID: {result['file_id']}")
                
#                 if failed:
#                     print(f"\nFailed: {len(failed)}")
#                     for result in failed:
#                         print(f"  ✗ {Path(result['file_path']).name}")
#                         print(f"    Error: {result['error']}")
                
#                 # Save file IDs
#                 self.uploaded_files.update(self.provider.get_uploaded_files_info())
#                 with open(self.uploaded_filepath, 'w') as f:
#                     json.dump(self.uploaded_files, f, indent=2)
                
#                 print(f"\n📝 File IDs saved to: {self.uploaded_filepath}")
#             else:
#                 print(f"\n📝 Files already uploaded, skipping")
#                 all_uploaded = True
            
#             self.step_results['upload_files'] = {
#                 'status': 'success' if all_uploaded else 'failed',
#                 'name': 'Upload Files'
#             }
#             return all_uploaded
            
#         except Exception as e:
#             print(f"✗ Upload failed: {e}")
#             import traceback
#             traceback.print_exc()
#             self.step_results['upload_files'] = {
#                 'status': 'error',
#                 'name': 'Upload Files',
#                 'error': str(e)
#             }
#             return False
    
#     def execute_task(self):
#         """Execute task using provider"""
#         print("\n" + "=" * 70)
#         print(f"TASK EXECUTION ({self.provider_id.upper()})")
#         print("=" * 70 + "\n")
        
#         # prompt_path = "for_code_execution/code_execution_prompt.txt"
#         prompt_path = "for_code_execution_openai/code_execution_prompt.txt"
        
#         try:
#             # Get file IDs
#             file_ids = [info['file_id'] for info in self.uploaded_files.values()] if self.uploaded_files else []
            
#             # Create conversation using provider
#             conversation_id = int(datetime.datetime.now().timestamp() * 1000)
            
#             # OpenAI needs file_ids at conversation creation
#             if self.provider_id == 'openai':
#                 self.provider.create_conversation(conversation_id, file_ids)
#             else:
#                 self.provider.create_conversation(conversation_id)
            
#             # Start conversation
#             result = self.provider.start_conversation(file_ids, prompt_path)
#             result["conversation_id"] = conversation_id
            
#             # Track token usage
#             if result['success'] and 'token_usage' in result:
#                 self.total_tokens['input'] += result['token_usage']['input_tokens']
#                 self.total_tokens['output'] += result['token_usage']['output_tokens']
#                 self.total_tokens['total'] += result['token_usage']['total_tokens']
#                 self.total_cost += result['estimated_cost']['total']
            
#             if result['success']:
#                 # Download artifacts
#                 downloaded = self._extract_and_download_artifacts(result, step_name='step_06')
#                 self.downloaded_files.update(downloaded)
#                 print(f"\n✓ Downloaded {len(downloaded)} files")
            
#             # Save logs
#             self.provider.save_conversation_log(f'{self.output_dir}/execution_log.json')
            
#             self.step_results['execute_task'] = {
#                 'status': 'success' if result['success'] else 'failed',
#                 'name': 'Task Execution'
#             }
            
#             # Save response
#             with open(Path(f'{self.output_dir}/execution_response.json'), 'w') as f:
#                 json.dump(result, f, indent=2)
            
#             return result['success']
            
#         except Exception as e:
#             print(f"✗ Execution failed: {e}")
#             import traceback
#             traceback.print_exc()
#             self.step_results['execute_task'] = {
#                 'status': 'error',
#                 'name': 'Task Execution',
#                 'error': str(e)
#             }
#             return False
    
#     def _extract_and_download_artifacts(self, result, step_name: str) -> dict:
#         """Extract and download artifacts using provider"""
#         downloads_dir = Path(f'{self.output_dir}/{step_name}')
        
#         if self.provider_id == 'claude':
#             file_ids = extract_file_ids_from_response(result)
#             print(f"{"~^"* 50}\n\n {file_ids = }\n\n{"~^"* 50}")
            
#             if not file_ids:
#                 print(f"⚠️  No file_ids found in response for {step_name}")
#                 return {}
            
#             downloaded = self.provider.download_multiple_artifacts(file_ids, downloads_dir)
        
#         elif self.provider_id == 'openai':
#             container_id = result['container_id']
#             downloaded = self.provider.download_all_container_files(container_id, downloads_dir)
        
#         return downloaded
    
#     def run_all_steps(self):
#         """Run all pipeline steps"""
#         self.print_banner()
        
#         # Upload files
#         if not self.upload_files():
#             print("\n⚠ File upload failed. Cannot proceed.")
#             self.print_summary()
#             return 1
        
#         # Execute task
#         if not self.execute_task():
#             print("\n⚠ Execution failed. Cannot proceed.")
#             self.print_summary()
#             return 1
        
#         # Print summary
#         self.print_summary()
        
#         print("✓ Pipeline execution completed!")
#         return 0
    
#     def print_banner(self):
#         """Print pipeline banner"""
#         print("\n" + "=" * 70)
#         print("REPOSITORY CONTEXT GENERATION PIPELINE")
#         print("=" * 70)
#         print(f"Provider: {self.provider_id.upper()}")
#         print(f"Project Root: {self.project_root}")
#         print("=" * 70 + "\n")
    
#     def print_summary(self):
#         """Print execution summary"""
#         print("\n" + "=" * 70)
#         print("PIPELINE EXECUTION SUMMARY")
#         print("=" * 70 + "\n")
        
#         if not self.step_results:
#             print("No steps executed yet.")
#             return
        
#         for step_key, result in self.step_results.items():
#             status_icon = "✓" if result['status'] == 'success' else "✗"
#             print(f"  {status_icon} {result['name']}: {result['status']}")
#             if 'error' in result:
#                 print(f"     Error: {result['error']}")
        
#         # Print provider summary
#         if hasattr(self.provider, 'get_conversation_summary'):
#             self.provider.get_conversation_summary()
        
#         print("\n" + "=" * 70 + "\n")


# def main():
#     """Main entry point"""
#     import argparse
    
#     parser = argparse.ArgumentParser(description='Run conversation pipeline')
#     parser.add_argument('--provider', type=str, default='claude', 
#                        choices=['claude', 'openai'],
#                        help='LLM provider to use (default: claude)')
    
#     args = parser.parse_args()
    
#     orchestrator = PipelineOrchestrator(provider_id=args.provider)
#     return orchestrator.run_all_steps()


# if __name__ == "__main__":
#     sys.exit(main())



# -------------------------------------------------------------------------
"""
Main Entry Point for Axle Service
File: main.py
"""

import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

from services.axle import AxleService
# from services.data_exporter import DataExporter
from db import ReviewDataExtractor
from db import get_extractor

# Load environment variables
load_dotenv()

PROJECT_ROOT = Path(__file__).parent


def test_database_connection() -> bool:
    """
    Test database connection before running any operations.

    Returns:
        True if connection successful, False otherwise
    """
    print("\n" + "=" * 70)
    print("DATABASE CONNECTION TEST")
    print("=" * 70)
    print("Attempting to connect to database...")

    try:
        extractor = ReviewDataExtractor()
        print(f"  Host: {extractor._connection_params.get('host')}")
        print(f"  Port: {extractor._connection_params.get('port')}")
        print(f"  Database: {extractor._connection_params.get('database')}")
        print(f"  User: {extractor._connection_params.get('user')}")

        extractor.connect()
        print("\nDatabase connection successful!")
        extractor.disconnect()
        print("=" * 70)
        return True
    except Exception as e:
        print(f"\nDatabase connection failed: {e}")
        print("\nPlease check your .env file has correct database credentials:")
        print("  - DATABASE_URL or")
        print("  - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        print("=" * 70)
        return False


async def run_conversation_pipeline(args):
    """Run the conversation pipeline with LLM providers"""
    # Prepare file paths
    upload_files_directory = args.files_dir
    file_paths = [
        f"{upload_files_directory}/custom_instructions.md",
        f"{upload_files_directory}/output_format.md",
        f"{upload_files_directory}/project_context.md",
        f"{upload_files_directory}/review_guidelines.md"
    ]

    # Create Axle service
    axle_service = AxleService(project_root=PROJECT_ROOT)

    try:
        # Execute task
        result = await axle_service.execute_task(
            provider=args.provider,
            file_paths=file_paths,
            prompt_path=args.prompt
        )

        # Return exit code based on success
        return 0 if result['success'] else 1, result

    finally:
        # Cleanup
        await axle_service.cleanup()


def run_data_export(
    output_dir: Path = None, 
    cleanup: bool = True,
    min_files: int = None,
    max_files: int = None,
    pr_number: str = None,
    repository: str = None
) -> dict:
    """
    Export tenant data from database to tmp/poc folder structure.

    Creates per PR:
    - tmp/poc/agent_prompt.txt                     -> code execution prompt
    - tmp/poc/agent_prompt_template.yaml           -> file review template
    - tmp/poc/{pr_number}_{timestamp}/uploads/     -> zip files
    - tmp/poc/{pr_number}_{timestamp}/artifacts/   -> evaluation reports (later)
    - tmp/poc/{pr_number}_{timestamp}/metrics/     -> metrics files

    Args:
        output_dir: Directory to save exports (default: PROJECT_ROOT)
        cleanup: Remove unzipped folders after creating zips
        min_files: Minimum files per PR
        max_files: Maximum files per PR
        pr_number: Specific PR number to export
        repository: Repository for the specific PR
        
    Returns:
        Export result with export_dir and PR folder paths
    """
    output_dir = output_dir or PROJECT_ROOT
    extractor = get_extractor()
    
    if pr_number and repository:
        # Specific PR export
        return extractor.export_specific_pr(
            repository=repository,
            pr_number=pr_number,
            output_dir=output_dir,
            cleanup_folder=cleanup
        )
    else:
        # Bulk export
        return extractor.export_all_tenants(
            output_dir=output_dir, 
            cleanup_folder=cleanup,
            min_files=min_files,
            max_files=max_files
        )


async def run_audit_report_pipeline(args):
    """
    Run full audit report pipeline:
    1. Export tenant data from DB to tmp/poc folder structure
    2. Upload zips to API and run conversation for processing
    3. Save artifacts and metrics in the PR folder
    """
    print("\n" + "=" * 70)
    print("AUDIT REPORT PIPELINE")
    print("=" * 70)

    # Step 1: Export tenant data from database
    print("\nStep 1: Exporting tenant data from database...")
    export_result = run_data_export(
        output_dir=PROJECT_ROOT,
        cleanup=not args.keep_export_folder,
        min_files=args.pr_file_min,
        max_files=args.pr_file_max,
        pr_number=args.pr,
        repository=args.repo
    )

    if not export_result['success']:
        print("Export failed. Cannot proceed with audit report.")
        return 1, None

    # Check if we have any PRs
    if not export_result.get('pr_results'):
        print("No PRs found matching criteria.")
        return 0, None
    
    # === CRITICAL CHANGE: PROCESS ONLY ONE PR ===
    # The user request is to "take one pr for now".
    # We will take the first one from the filtered list.
    target_pr_result = export_result['pr_results'][0]
    target_pr_number = target_pr_result['pr_number']
    
    print(f"\nProcessing Single PR: {target_pr_number}")
    
    export_dir = export_result['export_dir']
    
    # Get PR folders info for THIS PR only
    pr_folders = {target_pr_number: export_result['pr_folders'][target_pr_number]}
    
    # Collect zip files ONLY for this PR
    zip_files = []
    zip_paths = target_pr_result.get('zip_paths', {})
    if 'files_zip' in zip_paths:
        zip_files.append(zip_paths['files_zip'])
    if 'logs_zip' in zip_paths:
        zip_files.append(zip_paths['logs_zip'])

    # Include repo context file if it was fetched from the database
    repo_context_file = target_pr_result.get('repo_context_file')
    if repo_context_file:
        print(f"  Repo context file: {repo_context_file}")

    print(f"  Zip files to upload for PR {target_pr_number}: {len(zip_files)}")

    # Collect prompt and template files from tmp/poc folder
    export_dir_path = Path(export_dir)
    prompt_files = []
    for ext in ['*.txt', '*.yaml', '*.yml', '*.md']:
        prompt_files.extend(export_dir_path.glob(ext))

    if prompt_files:
        print(f"  Prompt/template files found: {len(prompt_files)}")
        for pf in prompt_files:
            print(f"    - {pf.name}")

    # Step 2: Upload zips to API and run conversation
    print("\nStep 2: Uploading zips to API and running conversation...")

    # Prepare file paths - include:
    # 1. Exported PR zips (files.zip, logs.zip)
    # 2. Prompt/template files from tmp/poc folder
    file_paths = zip_files + [str(pf) for pf in prompt_files]

    # Add repo context from DB if available, otherwise fall back to static project_context.md
    if repo_context_file:
        file_paths.append(repo_context_file)
        print(f"  Using DB repo context: {Path(repo_context_file).name}")

    # Optionally add files from files_dir if they exist
    # Skip project_context.md if we already have the DB repo context
    upload_files_directory = Path(args.files_dir)
    additional_files = [
        "custom_instructions.md",
        "output_format.md",
        "review_guidelines.md"
    ]
    if not repo_context_file:
        additional_files.append("project_context.md")
    for filename in additional_files:
        filepath = upload_files_directory / filename
        if filepath.exists():
            file_paths.append(str(filepath))

    # Use the PR directory
    pr_dir = pr_folders[target_pr_number]['pr_dir']
    print(f"  Using PR directory: {pr_dir}")

    # Create Axle service with PR directory
    axle_service = AxleService(project_root=PROJECT_ROOT, pr_dir=pr_dir)

    try:
        result = await axle_service.execute_task(
            provider=args.provider,
            file_paths=file_paths,
            prompt_path=args.prompt
        )

        if result['success']:
            print("\nAudit report pipeline completed successfully!")
            print(f"  PR Directory: {result.get('pr_dir', 'N/A')}")
            print(f"  Artifacts: {result.get('artifacts_dir', 'N/A')}")
            print(f"  Metrics: {result.get('metrics_dir', 'N/A')}")
        else:
            print("\nAudit report pipeline failed.")

        return (0 if result['success'] else 1), export_dir

    finally:
        await axle_service.cleanup()


async def main():
    """Main async entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Axle Service - Conversation Pipeline & Audit Report Generator'
    )

    # Create subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # 'run' command - original conversation pipeline
    run_parser = subparsers.add_parser('run', help='Run conversation pipeline')
    run_parser.add_argument(
        '--provider',
        type=str,
        default='claude',
        choices=['claude', 'openai'],
        help='LLM provider to use (default: claude)'
    )
    run_parser.add_argument(
        '--files-dir',
        type=str,
        default='tmp/poc',
        help='Directory containing files to upload'
    )
    run_parser.add_argument(
        '--prompt',
        type=str,
        default='tmp/poc/agent_prompt.txt',
        help='Path to prompt file'
    )

    # 'export' command - export tenant data only
    export_parser = subparsers.add_parser('export', help='Export tenant data from database')
    export_parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for exports (default: project root)'
    )
    export_parser.add_argument(
        '--keep-folder',
        action='store_true',
        help='Keep export folder after creating zip'
    )
    export_parser.add_argument(
        '--pr-file-min',
        type=int,
        default=None,
        help='Minimum files per PR'
    )
    export_parser.add_argument(
        '--pr-file-max',
        type=int,
        default=None,
        help='Maximum files per PR'
    )
    export_parser.add_argument(
        '--pr',
        type=str,
        default=None,
        help='Specific PR number to export (requires --repo)'
    )
    export_parser.add_argument(
        '--repo',
        type=str,
        default=None,
        help='Repository name for the specific PR (requires --pr)'
    )

    # 'audit' command - full audit report pipeline
    audit_parser = subparsers.add_parser('audit', help='Run full audit report pipeline')
    audit_parser.add_argument(
        '--provider',
        type=str,
        default='claude',
        choices=['claude', 'openai'],
        help='LLM provider to use (default: claude)'
    )
    audit_parser.add_argument(
        '--files-dir',
        type=str,
        default='tmp/poc',
        help='Directory containing additional files to upload'
    )
    audit_parser.add_argument(
        '--prompt',
        type=str,
        default='tmp/poc/agent_prompt.txt',
        help='Path to audit prompt file'
    )
    audit_parser.add_argument(
        '--keep-export-folder',
        action='store_true',
        help='Keep export folder after creating zip'
    )
    audit_parser.add_argument(
        '--pr-file-min',
        type=int,
        default=None,
        help='Minimum files per PR'
    )
    audit_parser.add_argument(
        '--pr-file-max',
        type=int,
        default=None,
        help='Maximum files per PR'
    )
    audit_parser.add_argument(
        '--pr',
        type=str,
        default=None,
        help='Specific PR number to audit (requires --repo)'
    )
    audit_parser.add_argument(
        '--repo',
        type=str,
        default=None,
        help='Repository name for the specific PR (requires --pr)'
    )

    args = parser.parse_args()

    # Default to 'audit' if no command specified (exports DB data + runs conversation pipeline)
    if args.command is None:
        args.command = 'audit'
        args.provider = 'claude'
        args.files_dir = 'tmp/poc'
        args.prompt = 'tmp/poc/agent_prompt.txt'
        args.keep_export_folder = False
        args.pr_file_min = None
        args.pr_file_max = None
        args.pr = None
        args.repo = None

    # Test database connection first for commands that need it
    if args.command in ['export', 'audit']:
        if not test_database_connection():
            print("\nAborting: Cannot proceed without database connection.")
            return 1

    # Execute appropriate command
    if args.command == 'run':
        exit_code, _ = await run_conversation_pipeline(args)
        return exit_code

    elif args.command == 'export':
        output_dir = Path(args.output_dir) if args.output_dir else PROJECT_ROOT
        result = run_data_export(
            output_dir=output_dir, 
            cleanup=not args.keep_folder,
            min_files=args.pr_file_min,
            max_files=args.pr_file_max,
            pr_number=args.pr,
            repository=args.repo
        )
        return 0 if result['success'] else 1

    elif args.command == 'audit':
        exit_code, zip_path = await run_audit_report_pipeline(args)
        # zip_path contains the exported data zip for API upload
        return exit_code

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)