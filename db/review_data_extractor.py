#!/usr/bin/env python3
"""
Unified Review Data Extractor

Combines the best of extract_review_data_fixed.py and database.py:
- Class-based architecture with connection management
- Flexible filtering by repository, pr_number
- Optional date range or latest timestamp filtering
- File output or in-memory data return
- Proper schema prefix (code_review.)

Usage:
    from review_data_extractor import ReviewDataExtractor

    extractor = ReviewDataExtractor()

    # Get all reviews
    data = extractor.get_reviews()

    # Get latest PR reviews with all filters
    data = extractor.get_latest_pr_reviews(
        repository="owner/repo",
        pr_number="123"
    )

    # Extract to files
    extractor.extract_to_files(
        output_dir="./output",
        repository="owner/repo",
        pr_number="123"
    )
"""

import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


# Schema and table configuration
SCHEMA_NAME = "code_review"
TABLE_NAME = "review_eval_metrics"
FULL_TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_NAME}"
REPO_CONTEXT_TABLE_NAME = "repository_context"
FULL_REPO_CONTEXT_TABLE = f"{SCHEMA_NAME}.{REPO_CONTEXT_TABLE_NAME}"
# Column name in repository_context that holds the actual context text
REPO_CONTEXT_CONTENT_COLUMN = "context"


class ReviewDataExtractor:
    """
    Unified database extractor for review_eval_metrics table.

    Supports filtering by:
    - repository (string)
    - pr_number (string)
    - date range or latest timestamp
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database_url: Optional[str] = None
    ):
        """
        Initialize database connection.

        Can be configured via:
        1. Explicit parameters
        2. DATABASE_URL environment variable (or database_url param)
        3. Individual DB_* environment variables

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            database_url: Full PostgreSQL URL (takes precedence)
        """
        self._connection = None

        # Check for DATABASE_URL first
        db_url = database_url or os.getenv('DATABASE_URL')

        if db_url:
            self._connection_params = self._parse_database_url(db_url)
            print(f"Using DATABASE_URL for connection")
        else:
            self._connection_params = {
                'host': host or os.getenv('DB_HOST', 'localhost'),
                'port': port or int(os.getenv('DB_PORT', '5432')),
                'database': database or os.getenv('DB_NAME', 'audit_db'),
                'user': user or os.getenv('DB_USER', 'postgres'),
                'password': password or os.getenv('DB_PASSWORD', '')
            }
            print(f"Using individual connection parameters")

    @staticmethod
    def _parse_database_url(url: str) -> Dict[str, Any]:
        """
        Parse PostgreSQL database URL into connection parameters.

        Supports formats:
        - postgresql://user:password@host:port/database
        - postgres://user:password@host:port/database
        - postgresql+driver://user:password@host:port/database
        """
        original_url = url

        # Remove protocol prefix
        if url.startswith('postgresql+'):
            url = url.split('://', 1)[1]
        elif url.startswith('postgresql://'):
            url = url.replace('postgresql://', '', 1)
        elif url.startswith('postgres://'):
            url = url.replace('postgres://', '', 1)
        else:
            raise ValueError(
                f"Invalid database URL format. Must start with postgresql:// or postgres://\n"
                f"Got: {original_url[:30]}..."
            )

        # Split auth from location at last @
        if '@' not in url:
            raise ValueError("Database URL must include user:password@host")

        last_at_index = url.rfind('@')
        auth = url[:last_at_index]
        location = url[last_at_index + 1:]

        # Parse user:password
        if ':' not in auth:
            raise ValueError("Database URL must include user:password format")

        first_colon = auth.index(':')
        user = auth[:first_colon]
        password = auth[first_colon + 1:]

        # Parse host:port/database
        if '/' not in location:
            raise ValueError("Database URL must include database name")

        host_port, database = location.split('/', 1)

        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
        else:
            host = host_port
            port = '5432'

        return {
            'host': host,
            'port': int(port),
            'database': database,
            'user': user,
            'password': password
        }

    def connect(self) -> 'ReviewDataExtractor':
        """Establish database connection"""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self._connection_params)
            db = self._connection_params['database']
            host = self._connection_params['host']
            port = self._connection_params['port']
            print(f"Connected to PostgreSQL: {db}@{host}:{port}")
        return self

    def disconnect(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            print("Database connection closed")

    @contextmanager
    def cursor(self, cursor_factory=RealDictCursor):
        """Context manager for database cursor with auto-commit/rollback"""
        self.connect()
        cursor = self._connection.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise e
        finally:
            cursor.close()

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    # =========================================================================
    # Query Methods
    # =========================================================================

    def get_reviews(
        self,
        repository: Optional[str] = None,
        pr_number: Optional[str] = None,
        latest_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch review records with flexible filtering.

        Args:
            repository: Filter by repository (e.g., "owner/repo")
            pr_number: Filter by PR number
            latest_only: If True, only return records with the latest created_at
            limit: Maximum number of records to return

        Returns:
            List of review records as dictionaries
        """
        conditions = []
        params = []

        if repository:
            conditions.append("repository = %s")
            params.append(repository)

        if pr_number:
            conditions.append("pr_number = %s")
            params.append(str(pr_number))

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # If latest_only, find the latest timestamp where content exists
        if latest_only and conditions:
            latest_query = f"""
                SELECT MAX(created_at) as latest_timestamp
                FROM {FULL_TABLE_NAME}
                {where_clause}
                AND template_name != 'review_summary'
            """

            with self.cursor() as cursor:
                cursor.execute(latest_query, tuple(params))
                result = cursor.fetchone()
                latest_timestamp = result['latest_timestamp'] if result else None

                print(f"Latest timestamp: {latest_timestamp}")
                if not latest_timestamp:
                    print("No records found matching filters")
                    return []


                # Add timestamp filter
                conditions.append("created_at = %s")
                params.append(latest_timestamp)
                where_clause = "WHERE " + " AND ".join(conditions)

        # Build main query
        query = f"""
            SELECT
                id,

                request_id,
                repository,
                pr_number,
                file,
                status,
                timestamp,
                agent_selected_provider,
                agent_selected_model,
                agent_processing_mode_used,
                agent_cache_utilized,
                agent_reasoning,
                agent_confidence_score,
                agent_decision_time_ms,
                dynamic_prompt_chars,
                cache_prompt_chars,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_tokens,
                cost,
                error_code,
                error_message,
                llm_routing_service_response_time_ms,
                total_comments,
                total_suggestions,
                template_id,
                template_name,
                category_details,
                severity,
                ast_token_distribution,
                metadata_,
                prompt,
                llm_service_metrics,
                head_content,
                base_content,
                review_result,
                created_at,
                updated_at
            FROM {FULL_TABLE_NAME}
            {where_clause}
            ORDER BY created_at DESC, file
        """

        if limit:
            query += f" LIMIT {int(limit)}"

        with self.cursor() as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            print(f"Found {len(rows)} record(s)")
            return [dict(row) for row in rows]

    def get_latest_pr_reviews(
        self,
        repository: str,
        pr_number: str
    ) -> List[Dict[str, Any]]:
        """
        Convenience method: Get latest reviews for a specific PR.

        Args:
            repository: Repository name (e.g., "owner/repo")
            pr_number: PR number

        Returns:
            List of review records with the latest timestamp
        """
        return self.get_reviews(
            repository=repository,
            pr_number=pr_number,
            latest_only=True
        )

    def get_repo_id_by_name(self, repository: str) -> Optional[str]:
        """
        Look up the repo_id from repository_context using metadata_->>'repo_full_name'.

        Args:
            repository: Repository name (e.g., owner/repo)

        Returns:
            repo_id (UUID string) or None if not found
        """
        query = f"""
            SELECT repo_id
            FROM {FULL_REPO_CONTEXT_TABLE}
            WHERE metadata_->>'repo_full_name' = %s
              AND status = 'COMPLETED'
            ORDER BY created_at DESC
            LIMIT 1
        """
        try:
            with self.cursor() as cursor:
                cursor.execute(query, (repository,))
                result = cursor.fetchone()
                if result:
                    return result['repo_id']
                return None
        except Exception as e:
            print(f"  Warning: Failed to fetch repo_id for '{repository}': {e}")
            return None

    def get_pr_first_seen_time(self, repository: str, pr_number: str) -> Optional[datetime]:
        """
        Get the MIN(created_at) for a specific PR across all historical records.
        This represents when the PR was first reviewed/seen.
        """
        print(f"  Fetching first seen time for {repository} PR #{pr_number}...")
        query = f"""
            SELECT MIN(created_at) as first_seen
            FROM {FULL_TABLE_NAME}
            WHERE repository = %s AND pr_number = %s
        """
        try:
            with self.cursor() as cursor:
                cursor.execute(query, (repository, str(pr_number)))
                result = cursor.fetchone()
                if result and result['first_seen']:
                    first_seen = result['first_seen']
                    
                    from datetime import timezone
                    # Convert to GMT format if it's naive or in another timezone
                    if first_seen.tzinfo is None:
                        first_seen = first_seen.replace(tzinfo=timezone.utc)
                    first_seen_gmt = first_seen.astimezone(timezone.utc)
                    
                    print(f"  First seen timestamp (GMT): {first_seen_gmt.strftime('%Y-%m-%d %H:%M:%S.%f%z')}")
                    return first_seen_gmt
                print(f"  No records found for {repository} PR #{pr_number}")
                return None
        except Exception as e:
            print(f"  Warning: Failed to fetch first seen time for PR {pr_number}: {e}")
            return None

    def get_context_from_review_metrics(
        self,
        repository: str,
        pr_number: str,
        review_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Fetch project_context from review_eval_metrics.metadata_ by matching
        repository, pr_number and status = 'success'.
        Optionally narrows to a specific review_id for a more precise match.

        Returns:
            project_context string or None if not found.
        """
        conditions = [
            "repository = %s",
            "pr_number = %s",
            "status = 'success'",
        ]
        params: list = [repository, str(pr_number)]

        if review_id:
            conditions.append("review_id = %s")
            params.append(review_id)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT metadata_->>'project_context' AS project_context
            FROM {FULL_TABLE_NAME}
            WHERE {where_clause}
            LIMIT 1
        """

        log_suffix = f", review_id='{review_id}'" if review_id else ""
        try:
            print(f"  Checking review_eval_metrics for project_context (repository='{repository}', pr_number='{pr_number}'{log_suffix})...")
            with self.cursor() as cursor:
                cursor.execute(query, tuple(params))
                result = cursor.fetchone()
                if result and result.get('project_context'):
                    print(f"  Found project_context in review_eval_metrics.")
                    return result['project_context']
                print(f"  No project_context found in review_eval_metrics.")
                return None
        except Exception as e:
            print(f"  Warning: Failed to fetch project_context from review_eval_metrics: {e}")
            return None

    def get_repo_context(
        self,
        repository,
        repo_id: str,
        pr_earliest_created_at: datetime
    ) -> Optional[str]:
        """
        Fetch the most recent repository context available at or before the PR was reviewed.

        Logic:
        - Match repository_context.repo_id to the provided repo_id
        - Only consider rows with status = 'COMPLETED'
        - Only consider rows created at or before pr_earliest_created_at (no future snapshots)
        - Return the latest such snapshot (closest in the past to when the PR was reviewed)

        Args:
            repo_id: Repository ID
            pr_earliest_created_at: Earliest created_at timestamp among the PR's review records
                                    (represents when the PR review run started)

        Returns:
            Context content string or None if not found.
            NOTE: The content column is configured via REPO_CONTEXT_CONTENT_COLUMN (default: 'context').
                  Update that constant if your table uses a different column name.
        """
        from datetime import timezone
        
        # Ensure the earliest created_at timestamp is in GMT format
        if pr_earliest_created_at.tzinfo is None:
            pr_earliest_created_at = pr_earliest_created_at.replace(tzinfo=timezone.utc)
        pr_earliest_created_at_gmt = pr_earliest_created_at.astimezone(timezone.utc)
        
        # Format explicitly to string for logging and query, including microseconds
        gmt_time_str = pr_earliest_created_at_gmt.strftime('%Y-%m-%d %H:%M:%S.%f+00:00')

        # Compare created_at directly as timestamptz (repository_context.created_at is TIMESTAMPTZ)
        # Do NOT use AT TIME ZONE 'UTC' here — on a TIMESTAMPTZ column it strips timezone info
        # and returns a naive TIMESTAMP, which PostgreSQL then re-interprets using the session
        # timezone, potentially producing wrong comparisons in non-UTC sessions.
        query = f"""
            SELECT {REPO_CONTEXT_CONTENT_COLUMN}
            FROM {FULL_REPO_CONTEXT_TABLE}
            WHERE repo_id = %s
              AND status = 'COMPLETED'
              AND created_at <= %s::timestamptz
            ORDER BY created_at DESC
        """

        try:
            print(f" ~~Fetching repo context for repo_id '{repo_id}' at or before {gmt_time_str} (GMT)...")
            
            with self.cursor() as cursor:
                cursor.execute(query, (repo_id, gmt_time_str))
                result = cursor.fetchall()
                if result:
                    
                    return result[0][REPO_CONTEXT_CONTENT_COLUMN]
                print(f"  No repo context found for repo_id '{repo_id}' at or before {pr_earliest_created_at}")
                return None
        except Exception as e:
            print(f"  Warning: Failed to fetch repo context for repo_id '{repo_id}': {e}")
            return None

    def export_specific_pr(
        self,
        repository: str,
        pr_number: str,
        output_dir: Optional[Path] = None,
        cleanup_folder: bool = True,
        review_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Export data for a specific PR, including repo context.

        Args:
            repository: Repository name
            pr_number: PR number
            output_dir: Output directory
            
        Returns:
            Export result dictionary
        """
        print("\n" + "=" * 70)
        print(f"DATA EXPORT - Specific PR: {repository} #{pr_number}")
        print("=" * 70)

        # Create export directory
        self._create_export_directory(output_dir)
        
        # 1. Fetch Reviews (using latest_only=True as per typical requirement for single PR)
        # We can use get_reviews directly. 
        # Assuming repo+pr is unique enough or we want all.
        
        reviews = self.get_reviews(
            repository=repository,
            pr_number=pr_number,
            latest_only=True # Fetching latest reviews for this PR
        )
        
        if not reviews:
            print(f"No reviews found for PR {pr_number} in {repository}")
            return {
                'success': False, 
                'message': 'No reviews found',
                'export_dir': str(self.export_dir)
            }
            
        # 2. Fetch Repo Context
        # Use the absolute earliest created_at for this PR across all historical records.
        # This matches MIN(created_at) representing when the PR was truly first reviewed/created.
        pr_earliest_created_at = self.get_pr_first_seen_time(repository, pr_number)

        if not pr_earliest_created_at:
            # Fallback to the fetched reviews if the DB query fails for some reason
            pr_earliest_created_at = min(r['created_at'] for r in reviews if r.get('created_at'))

        # 2b. First check review_eval_metrics.metadata_ for project_context
        context = self.get_context_from_review_metrics(repository, pr_number, review_id=review_id)

        if not context:
            # Fallback: look up repo_id and query repository_context table
            print(f"  Fetching repo_id for '{repository}'...")
            repo_id = self.get_repo_id_by_name(repository)

            if not repo_id:
                print(f"  Warning: Could not find valid repo_id for '{repository}' in repository_context table")
            else:
                print(f"  Fetching repo context for repo_id '{repo_id}' (PR earliest: {pr_earliest_created_at})...")
                context = self.get_repo_context(repository, repo_id, pr_earliest_created_at)
            
        
        # 3. Create Directories
        # We use a slight variant of directory creation since we know the repo
        # We can use the standard one, it handles the folder naming.
        # We might want to force a folder name structure though? 
        # _create_pr_directories uses {pr_number}_{timestamp}_{repo_suffix}
        # Let's rely on it.
        
        repo_suffix = self._shorten_repo_name(repository)
        pr_dir, uploads_dir, files_dir, log_files_dir, metrics_dir = self._create_pr_directories(
            pr_number, repo_suffix
        )
        
        # 4. Write Repo Context
        repo_context_file = None
        if context:
            context_file = pr_dir / "project_context.txt"
            self._write_text_file(context, str(context_file))
            print(f"  Saved project context to: {context_file.name}")
            repo_context_file = str(context_file)
        else:
            print("  No project context found.")

        # 5. Export Files
        result = self._export_pr_data(
            pr_number, reviews, metrics_dir, files_dir, log_files_dir, uploads_dir
        )

        # Cleanup unzipped folders (keep only the zip files)
        if cleanup_folder:
            if files_dir.exists():
                shutil.rmtree(files_dir)
            if log_files_dir.exists():
                shutil.rmtree(log_files_dir)
            print(f"  Cleaned up unzipped folders (kept zip files)")

        result['pr_dir'] = str(pr_dir)
        result['repository'] = repository
        result['artifacts_dir'] = str(pr_dir / "artifacts")
        result['metrics_dir'] = str(metrics_dir)
        result['repo_context_file'] = repo_context_file

        # Return structured like export_all_recent_prs returns mostly for consistency
        # wrapping in a structure that 'run_audit_report_pipeline' can consume if needed
        # It expects 'pr_results' list.

        return {
            'success': True,
            'message': 'Specific PR export completed',
            'export_dir': str(self.export_dir),
            'pr_count': 1,
            'pr_results': [result],
            'pr_folders': {
                pr_number: {
                    'pr_number': pr_number,
                    'repository': repository,
                    'pr_dir': str(pr_dir),
                    'uploads_dir': str(uploads_dir),
                    'repo_context_file': repo_context_file
                }
            }
        }

    # =========================================================================
    # POC Export Methods (Migrated from DataExporter)
    # =========================================================================

    def _create_export_directory(self, output_dir: Path) -> Path:
        """Create tmp/poc export directory, preserving existing files"""
        self.export_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.export_timestamp_ms = int(datetime.now().timestamp() * 1000)
        
        # Use provided output_dir or default to cwd
        base_dir = output_dir if output_dir else Path.cwd()
        self.export_dir = base_dir / "tmp" / "poc"
        
        # Create directory if it doesn't exist, but DON'T delete existing files
        self.export_dir.mkdir(parents=True, exist_ok=True)
        print(f"Using export directory: {self.export_dir}")
        return self.export_dir

    def _create_pr_directories(self, pr_number: str, repo_suffix: Optional[str] = None) -> tuple[Path, Path, Path, Path, Path]:
        """
        Create uploaded_to_eval_agent, reports_generated, and metrics directories for a specific PR.
        Using format: {pr_number}_{timestamp} or {pr_number}_{timestamp}_{repo_suffix} if repo_suffix provided

        Args:
            pr_number: The PR number
            repo_suffix: Optional shortened repository name to add to folder (for disambiguation)
        """
        # Create folder name with PR number and timestamp
        if repo_suffix:
            folder_name = f"{pr_number}_{self.export_timestamp_ms}_{repo_suffix}"
        else:
            folder_name = f"{pr_number}_{self.export_timestamp_ms}"

        pr_dir = self.export_dir / folder_name
        uploads_dir = pr_dir / "uploaded_to_eval_agent"
        files_dir = uploads_dir / "files"  # Temp dir for files before zipping
        log_files_dir = uploads_dir / "log-files"  # Temp dir for logs before zipping
        metrics_dir = pr_dir / "metrics"
        artifacts_dir = pr_dir / "reports_generated"

        # Clear existing directories for fresh export
        if pr_dir.exists():
            shutil.rmtree(pr_dir)

        # Create all directories
        files_dir.mkdir(parents=True, exist_ok=True)
        log_files_dir.mkdir(parents=True, exist_ok=True)
        metrics_dir.mkdir(parents=True, exist_ok=True)
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        return pr_dir, uploads_dir, files_dir, log_files_dir, metrics_dir

    def _export_report_files(
        self,
        files_dir: Path,
        log_files_dir: Path,
        report: Dict[str, Any],
        index: int,
        resolved_name: Optional[str] = None
    ) -> Dict[str, int]:
        """Export files for a single report to the appropriate directories.

        Args:
            files_dir: Directory for source files (head_content)
            log_files_dir: Directory for log files (metrics, prompt, response)
            report: Report data dictionary
            index: 1-based index of this report
            resolved_name: Pre-resolved unique filename (handles duplicates). If None, falls back to extraction.
        """
        file_path = report.get('file', '')

        # Use resolved name if provided (handles duplicates), otherwise extract from path
        if resolved_name:
            base_name = self._sanitize_filename(resolved_name)
        else:
            base_name = self._extract_filename_from_path(file_path)
            base_name = self._sanitize_filename(base_name) if base_name else f"record_{index}"

        # Determine extension from original file path, default to .txt if none/unknown
        file_ext = ".txt"
        if file_path:
            ext = Path(file_path).suffix
            if ext:
                file_ext = ext

        files_created = {'files': 0, 'log_files': 0}

        # === FILES FOLDER: head_content (actual file content) ===
        head_content = report.get('head_content')
        if head_content:
            # We use _write_text_file but need to adapt it to take Path or convert Path to str
            self._write_text_file(head_content, str(files_dir / f"{base_name}{file_ext}"))
            files_created['files'] += 1

        # === LOG-FILES FOLDER: metrics, prompt, response ===

        # Write metrics JSON (llm_service_metrics)
        metrics = self._parse_json_field(report.get('llm_service_metrics'))
        if metrics:
            self._write_json_file(metrics, str(log_files_dir / f"{base_name}_metrics.json"))
            files_created['log_files'] += 1

        # Write prompt text
        prompt = report.get('prompt')
        if prompt:
            self._write_text_file(prompt, str(log_files_dir / f"{base_name}_prompt.txt"))
            files_created['log_files'] += 1

        # Write review result JSON (response)
        review_result = self._parse_json_field(report.get('review_result'))
        if review_result:
            self._write_json_file(review_result, str(log_files_dir / f"{base_name}_response.json"))
            files_created['log_files'] += 1

        return files_created

    def _create_zip_archives(self, uploads_dir: Path, files_dir: Path, log_files_dir: Path) -> Dict[str, Path]:
        """Create files.zip and log-files.zip in the uploads folder."""
        zip_paths = {}

        # Create files.zip in uploads folder
        if any(files_dir.iterdir()):
            files_zip = shutil.make_archive(
                base_name=str(uploads_dir / "files"),
                format='zip',
                root_dir=files_dir,
                base_dir='.'
            )
            zip_paths['files_zip'] = Path(files_zip)

        # Create log-files.zip in uploads folder
        if any(log_files_dir.iterdir()):
            logs_zip = shutil.make_archive(
                base_name=str(uploads_dir / "log-files"),
                format='zip',
                root_dir=log_files_dir,
                base_dir='.'
            )
            zip_paths['logs_zip'] = Path(logs_zip)

        return zip_paths

    def _export_pr_data(
        self,
        pr_number: str,
        reports: List[Dict[str, Any]],
        metrics_dir: Path,
        files_dir: Path,
        log_files_dir: Path,
        uploads_dir: Path
    ) -> Dict[str, Any]:
        """Export all report data for a single PR."""
        print(f"\n  Exporting PR: {pr_number} ({len(reports)} reports)")

        # Resolve duplicate filenames by prepending parent/component name
        resolved_names = self._resolve_duplicate_filenames(reports)

        # Log any resolved duplicates for visibility
        original_stems = [Path(r.get('file', '')).stem for r in reports]
        stem_counts = {}
        for s in original_stems:
            stem_counts[s] = stem_counts.get(s, 0) + 1
        duplicates = {s for s, c in stem_counts.items() if c > 1 and s}
        if duplicates:
            print(f"    Resolved {len(duplicates)} duplicate filename(s): {', '.join(sorted(duplicates))}")

        # Export each report
        total_files = 0
        total_log_files = 0
        exported_items = []

        for idx, report in enumerate(reports, start=1):
            file_path = report.get('file', f'record_{idx}')

            # Use resolved name (0-based index in resolved_names)
            resolved_name = resolved_names.get(idx - 1, '')
            base_name = self._sanitize_filename(resolved_name) if resolved_name else f"record_{idx}"

            counts = self._export_report_files(files_dir, log_files_dir, report, idx, resolved_name=resolved_name or None)
            total_files += counts['files']
            total_log_files += counts['log_files']

            # NOTE: Metrics export to metrics folder REMOVED per user request

            exported_items.append({
                'file': file_path,
                'base_name': base_name,
                'files_created': counts['files'],
                'log_files_created': counts['log_files']
            })

        print(f"    uploads/files/: {total_files} files (head_content)")
        print(f"    uploads/log-files/: {total_log_files} files (metrics, prompt, response)")

        # Create zip archives
        zip_paths = self._create_zip_archives(uploads_dir, files_dir, log_files_dir)

        if zip_paths.get('files_zip'):
            print(f"    Created uploads/files.zip")
        if zip_paths.get('logs_zip'):
            print(f"    Created uploads/log-files.zip")

        return {
            'pr_number': pr_number,
            'report_count': len(reports),
            'files_count': total_files,
            'log_files_count': total_log_files,
            'zip_paths': {k: str(v) for k, v in zip_paths.items()},
            'exported_items': exported_items
        }

        # NOTE: export_all_recent_prs method removed to strictly enforce PR/Repository level extraction design

    # =========================================================================
    # File Output Methods
    # =========================================================================

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Remove invalid filesystem characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename

    @staticmethod
    def _extract_filename_from_path(file_path: str) -> str:
        """Extract filename without extension from a path"""
        if not file_path:
            return ""
        return Path(file_path).stem

    @staticmethod
    def _resolve_duplicate_filenames(reports: List[Dict[str, Any]]) -> Dict[int, str]:
        """
        Detect duplicate filenames across reports and resolve them by
        prepending the parent directory or component name.

        For example:
          src/components/Button/index.tsx  -> Button_index
          src/utils/index.tsx              -> utils_index
          src/helpers/format.ts            -> format  (no conflict, unchanged)

        If parent names also collide, walks further up the path:
          src/v1/utils/index.ts  -> v1_utils_index
          src/v2/utils/index.ts  -> v2_utils_index

        Args:
            reports: List of report dicts, each with a 'file' field

        Returns:
            Dict mapping report index (0-based) to resolved base_name (no extension)
        """
        # Step 1: Build list of (index, file_path, stem)
        entries = []
        for idx, report in enumerate(reports):
            file_path = report.get('file', '')
            stem = Path(file_path).stem if file_path else ''
            entries.append((idx, file_path, stem))

        # Step 2: Find which stems are duplicated
        stem_counts: Dict[str, List[int]] = {}
        for idx, file_path, stem in entries:
            key = stem.lower() if stem else f"__empty_{idx}"
            if key not in stem_counts:
                stem_counts[key] = []
            stem_counts[key].append(idx)

        duplicated_stems = {stem for stem, indices in stem_counts.items() if len(indices) > 1}

        # Step 3: Resolve duplicates by prepending parent path components
        resolved: Dict[int, str] = {}

        for idx, file_path, stem in entries:
            if not stem:
                resolved[idx] = ""
                continue

            stem_key = stem.lower()
            if stem_key not in duplicated_stems:
                # No conflict - use stem as-is
                resolved[idx] = stem
                continue

            # Get path parts (excluding the filename itself)
            path_obj = Path(file_path)
            parent_parts = list(path_obj.parent.parts)

            # Try adding parent components one at a time until unique
            # Start with 1 parent, then 2, etc.
            unique_name = stem
            for depth in range(1, len(parent_parts) + 1):
                prefix_parts = parent_parts[-depth:]
                candidate = "_".join(prefix_parts) + "_" + stem
                # Sanitize any path separators in parent parts
                candidate = candidate.replace("/", "_").replace("\\", "_")

                # Check if this candidate is unique among all entries with same stem
                conflict_indices = stem_counts[stem_key]
                candidates_at_depth = []
                for cidx in conflict_indices:
                    c_file_path = entries[cidx][1]
                    c_stem = entries[cidx][2]
                    c_parent_parts = list(Path(c_file_path).parent.parts)
                    if depth <= len(c_parent_parts):
                        c_prefix = "_".join(c_parent_parts[-depth:])
                        c_candidate = c_prefix + "_" + c_stem
                    else:
                        c_candidate = "_".join(c_parent_parts) + "_" + c_stem if c_parent_parts else c_stem
                    candidates_at_depth.append(c_candidate.replace("/", "_").replace("\\", "_"))

                # Check if all candidates at this depth are unique
                if len(set(c.lower() for c in candidates_at_depth)) == len(candidates_at_depth):
                    unique_name = candidate
                    break
                else:
                    unique_name = candidate  # Use it anyway, will try deeper

            resolved[idx] = unique_name

        return resolved

    @staticmethod
    def _shorten_repo_name(repository: str) -> str:
        """
        Shorten repository name for folder naming.
        Example: "owner/my-awesome-repo" -> "my-awesome-repo"
                 "7thSense-Dev/codeweaver" -> "codeweaver"

        Args:
            repository: Full repository name (e.g., "owner/repo")

        Returns:
            Shortened repository name (just the repo part)
        """
        if not repository:
            return "unknown"

        # Remove owner prefix, keep only repo name
        if '/' in repository:
            return repository.split('/')[-1]

        return repository

    @staticmethod
    def _is_valid_pr_number(pr_number: str) -> bool:
        """
        Validate that PR number is numeric (not a UUID or other invalid format).

        Args:
            pr_number: PR number to validate

        Returns:
            True if valid numeric PR number, False otherwise
        """
        if not pr_number or pr_number == 'unknown':
            return False

        # Check if it's numeric (allowing leading zeros)
        if pr_number.isdigit():
            return True

        # Reject UUIDs (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        if '-' in pr_number and len(pr_number) >= 32:
            return False

        # Reject other non-numeric formats
        return False

    @staticmethod
    def _write_json_file(data: Any, filepath: str) -> None:
        """Write data to JSON file with pretty formatting"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        print(f"  Created: {filepath}")

    @staticmethod
    def _write_text_file(content: str, filepath: str) -> None:
        """Write text content to file"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  Created: {filepath}")

    @staticmethod
    def _parse_json_field(data: Any) -> Optional[Dict]:
        """Parse JSON field that might be dict or string"""
        if data is None:
            return None
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return None
        return None

    def extract_to_files(
        self,
        output_dir: str,
        repository: Optional[str] = None,
        pr_number: Optional[str] = None,
        latest_only: bool = True,
        cleanup_folders: bool = True
    ) -> int:
        """
        Extract review data to zipped file bundles.

        Creates:
        - uploads/files.zip      - head_content (source files with original extensions)
        - uploads/log-files.zip  - metrics JSON, review result JSON, prompt text

        Args:
            output_dir: Directory for output files
            repository: Filter by repository
            pr_number: Filter by PR number
            latest_only: Only extract latest timestamp records
            cleanup_folders: Remove unzipped temp folders after creating zips

        Returns:
            Number of records processed
        """
        print("=" * 70)
        print("Review Data Extractor - File Export")
        print("=" * 70)

        output_path = Path(output_dir)
        uploads_dir = output_path / "uploads"
        files_dir = uploads_dir / "files"
        log_files_dir = uploads_dir / "log-files"

        # Create directories
        files_dir.mkdir(parents=True, exist_ok=True)
        log_files_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nOutput directory: {os.path.abspath(output_dir)}")

        # Log filters
        print("\nFilters applied:")
        if repository:
            print(f"  - repository: {repository}")
        if pr_number:
            print(f"  - pr_number: {pr_number}")
        if latest_only:
            print(f"  - latest_only: True")

        # Fetch data
        print("\nFetching data...")
        records = self.get_reviews(
            repository=repository,
            pr_number=pr_number,
            latest_only=latest_only
        )
        print(f"Total records fetched: {len(records)}")
        if not records:
            print("\nNo records found matching filters.")
            return 0

        print(f"\nProcessing {len(records)} record(s)...")

        # Resolve duplicate filenames across all records
        resolved_names = self._resolve_duplicate_filenames(records)

        # Log duplicates
        original_stems = [Path(r.get('file', '')).stem for r in records]
        stem_counts = {}
        for s in original_stems:
            stem_counts[s] = stem_counts.get(s, 0) + 1
        duplicates = {s for s, c in stem_counts.items() if c > 1 and s}
        if duplicates:
            print(f"  Resolved {len(duplicates)} duplicate filename(s): {', '.join(sorted(duplicates))}")

        # Process each record using the existing _export_report_files method
        total_files = 0
        total_log_files = 0

        for idx, record in enumerate(records, start=1):
            file_path = record.get('file', '')
            print(f"\n[{idx}/{len(records)}] {file_path or 'Unknown file'}")

            resolved_name = resolved_names.get(idx - 1, '')
            counts = self._export_report_files(
                files_dir, log_files_dir, record, idx,
                resolved_name=resolved_name or None
            )
            total_files += counts['files']
            total_log_files += counts['log_files']

        print(f"\n  uploads/files/: {total_files} files (head_content)")
        print(f"  uploads/log-files/: {total_log_files} files (metrics, prompt, response)")

        # Create zip archives
        zip_paths = self._create_zip_archives(uploads_dir, files_dir, log_files_dir)

        if zip_paths.get('files_zip'):
            print(f"  Created: uploads/files.zip")
        if zip_paths.get('logs_zip'):
            print(f"  Created: uploads/log-files.zip")

        # Cleanup unzipped temp folders
        if cleanup_folders:
            if files_dir.exists():
                shutil.rmtree(files_dir)
            if log_files_dir.exists():
                shutil.rmtree(log_files_dir)
            print(f"\n  Cleaned up unzipped folders (kept zip files)")

        print("\n" + "=" * 70)
        print(f"Export complete! {len(records)} record(s) processed.")
        print(f"Output: {os.path.abspath(output_dir)}")
        print(f"  uploads/files.zip      <- head_content (source files)")
        print(f"  uploads/log-files.zip  <- metrics, prompt, response")
        print("=" * 70)

        return len(records)


# =============================================================================
# Singleton instance for convenience
# =============================================================================

_extractor_instance: Optional[ReviewDataExtractor] = None


def get_extractor() -> ReviewDataExtractor:
    """Get or create singleton extractor instance"""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = ReviewDataExtractor()
    return _extractor_instance


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """Command-line interface for the extractor"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract review data from database to files"
    )
    parser.add_argument(
        '-r', '--repository',
        help='Filter by repository (e.g., owner/repo)'
    )
    parser.add_argument(
        '-p', '--pr-number',
        help='Filter by PR number'
    )
    parser.add_argument(
        '-o', '--output-dir',
        default='./extracted_data',
        help='Output directory (default: ./extracted_data)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Get all matching records (not just latest)'
    )
    parser.add_argument(
        '--keep-folders',
        action='store_true',
        help='Keep unzipped folders alongside zip files'
    )

    args = parser.parse_args()

    extractor = ReviewDataExtractor()

    try:
        extractor.extract_to_files(
            output_dir=args.output_dir,
            repository=args.repository,
            pr_number=args.pr_number,
            latest_only=not args.all,
            cleanup_folders=not args.keep_folders
        )
    finally:
        extractor.disconnect()


if __name__ == "__main__":
    main()
