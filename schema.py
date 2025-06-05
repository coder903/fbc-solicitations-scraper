SOLICITATIONS_SCHEMA = [
    # Core solicitation fields
    {"name": "solicitation_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "solicitation_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "return_by_date", "type": "DATETIME", "mode": "NULLABLE"},
    {"name": "posted_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
    
    # From batch quote file
    {"name": "solicitation_type", "type": "STRING", "mode": "NULLABLE"},  # F, P, I
    {"name": "small_business_setaside", "type": "STRING", "mode": "NULLABLE"},  # Y, N, H, R, L, A, E
    {"name": "setaside_percentage", "type": "FLOAT", "mode": "NULLABLE"},  # Percentage for set-aside (0-100)
    {"name": "bid_type", "type": "STRING", "mode": "NULLABLE"},  # BI, BW, AB, DQ (removed duplicate)
    {"name": "fob_point", "type": "STRING", "mode": "NULLABLE"},  # D, O
    {"name": "inspection_point", "type": "STRING", "mode": "NULLABLE"},  # D, O
    {"name": "additional_clause_fillins", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "discount_terms", "type": "STRING", "mode": "NULLABLE"},  # 1, 10, 3, 6, 8
    {"name": "days_quote_valid", "type": "INTEGER", "mode": "NULLABLE"},
    
    # AIDC contract terms (moved to solicitation level)
    {"name": "guaranteed_minimum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "do_minimum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "contract_maximum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "annual_frequency_buys", "type": "INTEGER", "mode": "NULLABLE"},
    
    # CLINs (Contract Line Item Numbers) - repeated/nested structure
    {
        "name": "clins",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "clin", "type": "STRING", "mode": "NULLABLE"},  # 0001, 0002, etc.
            {"name": "nsn", "type": "STRING", "mode": "NULLABLE"},  # 13-digit cleaned NSN
            {"name": "part_number", "type": "STRING", "mode": "NULLABLE"},  # Original with hyphens
            {"name": "nomenclature", "type": "STRING", "mode": "NULLABLE"},  # Item description
            {"name": "purchase_request", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pr_number", "type": "STRING", "mode": "NULLABLE"},  # Extracted from purchase_request
            {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "unit_of_issue", "type": "STRING", "mode": "NULLABLE"},  # EA, BX, etc.
            {"name": "unit_price", "type": "FLOAT", "mode": "NULLABLE"},  # From batch quote
            {"name": "delivery_days", "type": "INTEGER", "mode": "NULLABLE"},  # From batch quote
            {"name": "issued_date", "type": "DATE", "mode": "NULLABLE"},  # From web scrape
            {"name": "return_by", "type": "DATE", "mode": "NULLABLE"},  # From web scrape
            
            # Technical documents
            {
                "name": "technical_documents",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
            
            # Approved sources
            {
                "name": "approved_sources",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "supplier_cage_code", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "supplier_part_number", "type": "STRING", "mode": "NULLABLE"},
                ]
            }
        ]
    },
    
    # Additional fields from web scrape
    {"name": "setaside_type", "type": "STRING", "mode": "NULLABLE"},  # Descriptive text from web
    {"name": "rfq_quote_status", "type": "STRING", "mode": "NULLABLE"},
    
    # Links/URLs
    {
        "name": "links",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "solicitation_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "package_view_url", "type": "STRING", "mode": "NULLABLE"},
        ]
    },
    
    # Metadata
    {"name": "scrape_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "NULLABLE"},  # 'web_scrape' or 'batch_quote'
    
    # Store raw batch quote data for reference (optional)
    {"name": "raw_batch_quote_data", "type": "JSON", "mode": "NULLABLE"},
]