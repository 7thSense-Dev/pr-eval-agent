
import re
import json


def extract_file_ids_from_response(result):
    """
    Extract file_ids from Claude's response message
    
    Args:
        result: Response dictionary from create_message_with_files
        
    Returns:
        dict: {filename: file_id} mapping
    """
    file_ids = []
    
    if not result.get('success'):
        return file_ids
    
    message = result.get('message', {})
    content_blocks = message.get('content', [])
    
    for block in content_blocks:
        # Look for bash_code_execution_tool_result blocks
        if block.get('type') == 'bash_code_execution_tool_result':
            tool_content = block.get('content', {})
            output_files = tool_content.get('content', [])
            
            # Extract file_ids from output files
            for output_file in output_files:
                if output_file.get('type') == 'bash_code_execution_output':
                    file_id = output_file.get('file_id')
                    if file_id:
                        # We don't know the exact filename yet
                        # Store with generic name, will be mapped later
                        file_ids.append(file_id)
    
    return file_ids