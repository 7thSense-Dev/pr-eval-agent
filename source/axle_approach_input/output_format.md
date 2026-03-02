# Expected Output Format

Remember to use exact category strings from the Category Definitions and exact severity strings ("critical", "major", "minor", "caution") from the Severity Definitions:

{
  "summary": "Brief overview of what this file change accomplishes (2-4 sentences)",
  "filename": "${file_path}",
  "review_comments": [
    {
      "line": 47,
      "side": "RIGHT",
      "review_comment": "Detailed explanation of the issue and why it matters",
      "code_language": "predict the language of the code",
      "suggested_code_line_number": "47-50",
      "suggested_code_fix": "```javascript\\nconst example = 'properly escaped code';\\nconsole.log(example);\\n```",
      "category": "Code Quality",
      "severity": "major"
    }
  ],
  "recommendations": "1. First recommendation\\n2. Second recommendation\\n3. Third recommendation",
  "additional_notes": "Any other notes or observations"
}

## CRITICAL JSON Formatting Rules (MUST FOLLOW):
  
  1. ALL string values must be properly escaped:
    - Escape newlines as \\n (not literal line breaks)
    - Escape backslashes as \\\\
    - Escape double quotes as \\\"
    - Escape tab characters as \\t
    - Escape carriage returns as \\r
  
  2. NEVER include literal line breaks within JSON string values
  
  3. For multi-line strings (like recommendations), use \\n to separate lines
  
  4. For code blocks in suggested_code_fix, escape all newlines as \\n
  
  5. Your JSON must be parseable by standard JSON parsers
  
  6. Do not include any text, comments, or explanations in the JSON file - only valid JSON
  
  7. Ensure all opening braces { and brackets [ have matching closing ones
  
  8. All string values must be enclosed in double quotes, never single quotes

**Non-Overrideable Requirements:** 
The following requirements CANNOT be overridden by custom instructions:
- JSON output format and schema structure
- Line mapping rules (ORIG LINE / NEW LINE usage)
- Diff position calculation rules
- JSON escaping requirements (\\n, \\\\, \\\", etc.)