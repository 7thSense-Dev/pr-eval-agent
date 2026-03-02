"""
Axle Service - Orchestrates conversation pipeline operations
File: services/axle/axle_service.py
"""

import os
import json
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import structlog

from services.axle.adapters import AnthropicAxleAdapter, OpenAIAxleAdapter
from utils.parser import extract_file_ids_from_response

logger = structlog.get_logger()


class AxleService:
    """
    Axle Service - Orchestrates conversation pipeline with pre-initialized adapters

    Uses folder structure:
    - tmp/poc/{pr_number}_{timestamp}/
        - uploaded_to_eval_agent/   <- zip files (created by DataExporter)
        - reports_generated/        <- downloaded evaluation reports
        - metrics/                  <- conversation_log.json, execution_result.json
    """

    def __init__(self, project_root: Path = None, pr_dir: Optional[str] = None):
        """
        Initialize AxleService.

        Args:
            project_root: Project root directory
            pr_dir: Path to the PR directory (tmp/poc/{pr_number}_{timestamp})
                    If not provided, a default output directory will be created
        """
        self.project_root = project_root or Path.cwd()

        # Pre-initialize all adapters
        self.adapters = {
            "claude": AnthropicAxleAdapter(),
            "openai": OpenAIAxleAdapter()
        }

        # Current active adapter
        self.current_adapter = None

        # Setup output directories - use existing PR folder structure
        if pr_dir:
            # Use the provided PR directory structure (created by DataExporter)
            self.pr_dir = Path(pr_dir)
        else:
            # Fallback: use default path without creating new timestamped folder
            self.pr_dir = Path("tmp/poc/default")

        # Set up subdirectories within the existing PR folder
        self.artifacts_dir = self.pr_dir / "reports_generated"    # For eval reports
        self.metrics_dir = self.pr_dir / "metrics"                 # For metrics, conversation logs, execution results
        self.uploads_dir = self.pr_dir / "uploaded_to_eval_agent" # For uploaded zip files

        # Ensure directories exist (don't recreate if already present)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        # Don't create uploads_dir - it should already exist from DataExporter

        # File tracking
        self.uploaded_files = {}
        self.downloaded_files = {}
        self.uploaded_filepath = str(self.metrics_dir / "uploaded_files.json")
        self.downloaded_filepath = str(self.metrics_dir / "downloaded_files.json")

        logger.info(
            "Axle service initialized",
            adapters=list(self.adapters.keys()),
            pr_dir=str(self.pr_dir)
        )

        print(f"\n{'='*70}")
        print(f"Axle Service Initialized")
        print(f"{'='*70}")
        print(f"Available Adapters: {', '.join(self.adapters.keys()).upper()}")
        print(f"PR Directory       : {self.pr_dir}")
        print(f"Reports Directory  : {self.artifacts_dir}")
        print(f"Metrics Directory  : {self.metrics_dir}")
        print(f"{'='*70}\n")
    
    async def execute_task(
        self,
        provider: str,
        file_paths: List[str],
        prompt_path: str,
        conversation_id: int = None
    ) -> Dict[str, Any]:
        """
        Main orchestration method - handles upload, conversation, and download

        Args:
            provider: Provider to use ('claude' or 'openai')
            file_paths: List of file paths to upload
            prompt_path: Path to prompt file
            conversation_id: Optional conversation ID (auto-generated if not provided)

        Returns:
            Dict with execution results
        """
        if provider not in self.adapters:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(self.adapters.keys())}")

        # Set current adapter
        self.current_adapter = self.adapters[provider]

        # Generate conversation ID if not provided
        if conversation_id is None:
            conversation_id = int(datetime.datetime.now().timestamp() * 1000)

        result = {
            'provider': provider,
            'conversation_id': conversation_id,
            'steps': {},
            'success': False
        }

        try:
            # ===============================================================
            # STEP 1: UPLOAD FILES
            # ===============================================================
            print(f"\n{'='*70}")
            print(f"STEP 1: UPLOADING FILES TO {provider.upper()}")
            print(f"{'='*70}\n")

            upload_results = await self.current_adapter.upload_multiple_files(file_paths)

            successful_uploads = [r for r in upload_results if r['success']]
            failed_uploads = [r for r in upload_results if not r['success']]

            result['steps']['upload'] = {
                'total': len(upload_results),
                'successful': len(successful_uploads),
                'failed': len(failed_uploads),
                'success': len(failed_uploads) == 0
            }

            if failed_uploads:
                print(f"\n⚠️  {len(failed_uploads)} file(s) failed to upload")
                result['error'] = "File upload failed"
                return result

            # Save uploaded files info to metrics folder
            self.uploaded_files = self.current_adapter.get_uploaded_files_info()
            with open(self.uploaded_filepath, 'w') as f:
                json.dump(self.uploaded_files, f, indent=2)

            print(f"\n✓ All {len(successful_uploads)} files uploaded successfully")

            # ===============================================================
            # STEP 2: CREATE CONVERSATION
            # ===============================================================
            print(f"\n{'='*70}")
            print(f"STEP 2: CREATING CONVERSATION")
            print(f"{'='*70}\n")

            file_ids = [info['file_id'] for info in self.uploaded_files.values()]

            # OpenAI needs file_ids at conversation creation
            # Pass metrics_dir for logging
            if provider == 'openai':
                await self.current_adapter.create_conversation(conversation_id, file_ids, log_dir=self.metrics_dir)
            else:
                await self.current_adapter.create_conversation(conversation_id, log_dir=self.metrics_dir)

            result['steps']['conversation_init'] = {
                'success': True,
                'file_ids_count': len(file_ids)
            }

            # ===============================================================
            # STEP 3: START CONVERSATION
            # ===============================================================
            print(f"\n{'='*70}")
            print(f"STEP 3: STARTING CONVERSATION")
            print(f"{'='*70}\n")

            conversation_result = await self.current_adapter.start_conversation(file_ids, prompt_path)

            result['steps']['conversation'] = {
                'success': conversation_result['success'],
                'token_usage': conversation_result.get('token_usage', {}),
                'cost': conversation_result.get('estimated_cost', {})
            }

            if not conversation_result['success']:
                print(f"\n✗ Conversation failed")
                result['error'] = conversation_result.get('error', 'Unknown error')
                return result

            print(f"\n✓ Conversation completed successfully")

            # ===============================================================
            # STEP 4: DOWNLOAD ARTIFACTS
            # ===============================================================
            print(f"\n{'='*70}")
            print(f"STEP 4: DOWNLOADING ARTIFACTS")
            print(f"{'='*70}\n")

            # Download artifacts to artifacts folder
            downloads_dir = self.artifacts_dir

            if provider == 'claude':
                # Extract file_ids from response
                artifact_file_ids = extract_file_ids_from_response(conversation_result)

                if artifact_file_ids:
                    downloaded = await self.current_adapter.download_multiple_artifacts(
                        artifact_file_ids,
                        downloads_dir
                    )
                else:
                    downloaded = {}
                    print("⚠️  No artifacts found in response")

            elif provider == 'openai':
                # Download from container
                container_id = conversation_result.get('container_id')
                if container_id:
                    downloaded = await self.current_adapter.download_all_container_files(
                        container_id,
                        downloads_dir
                    )
                else:
                    downloaded = {}
                    print("⚠️  No container ID found")

            result['steps']['download'] = {
                'success': True,
                'files_downloaded': len(downloaded)
            }

            self.downloaded_files = downloaded

            print(f"\n✓ Downloaded {len(downloaded)} artifact(s)")

            # ===============================================================
            # STEP 5: SAVE LOGS TO METRICS FOLDER
            # ===============================================================
            # Save conversation log to metrics folder
            self.current_adapter.save_conversation_log(
                str(self.metrics_dir / 'conversation_log.json')
            )

            # Save complete result to metrics folder
            with open(self.metrics_dir / 'execution_result.json', 'w') as f:
                # Remove message object for clean JSON
                clean_result = conversation_result.copy()
                if 'message' in clean_result:
                    del clean_result['message']
                json.dump(clean_result, f, indent=2)

            result['success'] = True
            result['pr_dir'] = str(self.pr_dir)
            result['artifacts_dir'] = str(self.artifacts_dir)
            result['metrics_dir'] = str(self.metrics_dir)

            print(f"\n{'='*70}")
            print(f"✓ TASK EXECUTION COMPLETED SUCCESSFULLY")
            print(f"{'='*70}")
            print(f"\nPR Directory: {self.pr_dir}")
            print(f"Artifacts: {self.artifacts_dir}")
            print(f"Metrics: {self.metrics_dir}")
            print(f"Files Downloaded: {len(downloaded)}")
            print(f"{'='*70}\n")

            return result

        except Exception as e:
            logger.error("Task execution failed", error=str(e), provider=provider)
            print(f"\n✗ Task execution failed: {e}")
            import traceback
            traceback.print_exc()

            result['error'] = str(e)
            result['traceback'] = traceback.format_exc()
            return result

        finally:
            # Cleanup adapter logging
            if self.current_adapter:
                self.current_adapter.close_logging()
    
    async def cleanup(self):
        """Cleanup all adapters"""
        for provider, adapter in self.adapters.items():
            try:
                await adapter.cleanup()
                logger.info(f"Cleaned up {provider} adapter")
            except Exception as e:
                logger.error(f"Failed to cleanup {provider} adapter", error=str(e))