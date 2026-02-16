from datetime import date
from io import BytesIO
from typing import List, Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel


async def export_merged_va_records(
    current_user: dict,
    db: StandardDatabase,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[List[str]] = None,
    date_type: Optional[str] = None,
    results_filter: str = "all",  # "all", "both", "ccva_only", "pcva_only"
    file_format: str = "excel"
) -> StreamingResponse:
    """
    Export VA records merged with CCVA and PCVA results.
    
    Args:
        current_user: Current authenticated user
        db: ArangoDB database session
        start_date: Filter by start date
        end_date: Filter by end date
        locations: List of locations to filter by
        date_type: Type of date to filter (death_date, interview_date, submission_date)
        results_filter: Filter by result availability - "all", "both", "ccva_only", "pcva_only"
        file_format: Export format (excel or csv)
    
    Returns:
        StreamingResponse with Excel/CSV file
    """
    try:
        print(f"Export request received: start_date={start_date}, end_date={end_date}, locations={locations}, date_type={date_type}")
        
        # Fetch ODK configuration to get field mappings
        config = await fetch_odk_config(db, True)
        
        instance_id = config.field_mapping.instance_id or 'instanceid'
        region_field = config.field_mapping.location_level1 or 'region'
        district_field = config.field_mapping.location_level2 or 'district'
        interview_date_field = config.field_mapping.interview_date or 'id10012'
        interviewer_field = 'id10019'  # Interviewer name field
        death_date_field = config.field_mapping.death_date or 'id10023'
        submission_date_field = config.field_mapping.submitted_date or 'submissiondate'
        
        # Determine which date field to use for filtering
        date_field = death_date_field  # default
        if date_type == 'interview_date':
            date_field = interview_date_field
        elif date_type == 'submission_date':
            date_field = submission_date_field
        
        # Build filter conditions
        filter_conditions = []
        bind_vars = {}
        
        if start_date:
            filter_conditions.append(f"va.{date_field} >= @start_date")
            bind_vars['start_date'] = start_date.isoformat()
        
        if end_date:
            filter_conditions.append(f"va.{date_field} <= @end_date")
            bind_vars['end_date'] = end_date.isoformat()
        
        if locations and len(locations) > 0:
            filter_conditions.append(f"va.{region_field} IN @locations")
            bind_vars['locations'] = locations
        
        filter_clause = " AND ".join(filter_conditions) if filter_conditions else "true"
        
        # Add results_filter to bind vars
        bind_vars['results_filter'] = results_filter
        
        # Only apply limit for non-"both" filters (both filter naturally has fewer results)
        limit_clause = "LIMIT 300" if results_filter == "both" else "LIMIT 300"
        
        # AQL query to merge VA records with CCVA and PCVA results
        query = f"""
            FOR va IN {db_collections.VA_TABLE}
                FILTER {filter_clause}
                {limit_clause}
                
                // Get latest CCVA result for this VA
                LET ccva_result = FIRST(
                    FOR ccva IN {db_collections.CCVA_RESULTS}
                        FILTER ccva.ID == va.{instance_id} OR ccva.uid == va.{instance_id}
                        SORT ccva._key DESC
                        LIMIT 1
                        RETURN ccva
                )
                
                // Get latest PCVA result for this VA
                LET pcva_result = FIRST(
                    FOR pcva IN {db_collections.PCVA_RESULTS}
                        FILTER pcva.assigned_va == va.{instance_id}
                        SORT pcva.datetime DESC
                        LIMIT 1
                        
                        // Resolve ICD10 codes for PCVA
                        LET cause_a = FIRST(
                            FOR code IN {db_collections.ICD10}
                                FILTER code.uuid == pcva.frameA.a
                                RETURN CONCAT("(", code.code, ") ", code.name)
                        )
                        LET cause_b = FIRST(
                            FOR code IN {db_collections.ICD10}
                                FILTER code.uuid == pcva.frameA.b
                                RETURN CONCAT("(", code.code, ") ", code.name)
                        )
                        LET cause_c = FIRST(
                            FOR code IN {db_collections.ICD10}
                                FILTER code.uuid == pcva.frameA.c
                                RETURN CONCAT("(", code.code, ") ", code.name)
                        )
                        LET cause_d = FIRST(
                            FOR code IN {db_collections.ICD10}
                                FILTER code.uuid == pcva.frameA.d
                                RETURN CONCAT("(", code.code, ") ", code.name)
                        )
                        LET contributory_causes = (
                            FOR contrib_id IN (pcva.frameA.contributories || [])
                                FOR code IN {db_collections.ICD10}
                                    FILTER code.uuid == contrib_id
                                    RETURN CONCAT("(", code.code, ") ", code.name)
                        )
                        
                        RETURN {{
                            coder: pcva.created_by,
                            cause_a: cause_a,
                            cause_b: cause_b,
                            cause_c: cause_c,
                            cause_d: cause_d,
                            underlying_cause: cause_d != null ? cause_d :
                                            cause_c != null ? cause_c :
                                            cause_b != null ? cause_b :
                                            cause_a,
                            contributory_causes: CONCAT_SEPARATOR(", ", contributory_causes),
                            coded_at: pcva.datetime
                        }}
                )
                
                // Filter based on results availability
                FILTER (
                    @results_filter == "all" OR
                    (@results_filter == "both" AND ccva_result != null AND pcva_result != null) OR
                    (@results_filter == "ccva_only" AND ccva_result != null) OR
                    (@results_filter == "pcva_only" AND pcva_result != null)
                )
                
                RETURN {{
                    va_id: va.{instance_id},
                    region: va.{region_field},
                    district: va.{district_field},
                    interview_date: va.{interview_date_field},
                    death_date: va.{death_date_field},
                    submission_date: va.{submission_date_field},
                    interviewer_name: va.{interviewer_field},
                    
                    // CCVA Results
                    ccva_top_cause: ccva_result.CAUSE1,
                    ccva_cause2: ccva_result.CAUSE2,
                    ccva_cause3: ccva_result.CAUSE3,
                    ccva_likelihood: ccva_result.LIK1,
                    ccva_age_group: ccva_result.age_group,
                    ccva_gender: ccva_result.gender,
                    
                    // PCVA Results
                    pcva_coder: pcva_result.coder,
                    pcva_cause_a: pcva_result.cause_a,
                    pcva_cause_b: pcva_result.cause_b,
                    pcva_cause_c: pcva_result.cause_c,
                    pcva_cause_d: pcva_result.cause_d,
                    pcva_underlying_cause: pcva_result.underlying_cause,
                    pcva_contributory_causes: pcva_result.contributory_causes,
                    pcva_coded_at: pcva_result.coded_at
                }}
        """
        # print(query)
        # Execute query
        results = await VManBaseModel.run_custom_query(
            query=query,
            bind_vars=bind_vars,
            db=db
        )
        
        # Convert to list
        records = [record for record in results]
        
        if not records or len(records) == 0:
            raise HTTPException(
                status_code=404,
                detail="No records found matching the specified filters"
            )
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Rename columns for better readability
        column_names = {
            'va_id': 'VA ID',
            'region': 'Region',
            'district': 'District',
            'interview_date': 'Interview Date',
            'death_date': 'Death Date',
            'submission_date': 'Submission Date',
            'interviewer_name': 'Interviewer Name',
            'ccva_top_cause': 'CCVA - Top Cause',
            'ccva_cause2': 'CCVA - Cause 2',
            'ccva_cause3': 'CCVA - Cause 3',
            'ccva_likelihood': 'CCVA - Likelihood',
            'ccva_age_group': 'CCVA - Age Group',
            'ccva_gender': 'CCVA - Gender',
            'pcva_coder': 'PCVA - Coder',
            'pcva_cause_a': 'PCVA - Cause A',
            'pcva_cause_b': 'PCVA - Cause B',
            'pcva_cause_c': 'PCVA - Cause C',
            'pcva_cause_d': 'PCVA - Cause D',
            'pcva_underlying_cause': 'PCVA - Underlying Cause',
            'pcva_contributory_causes': 'PCVA - Contributory Causes',
            'pcva_coded_at': 'PCVA - Coded At'
        }
        df.rename(columns=column_names, inplace=True)
        
        # Fill NaN values with empty strings
        df.fillna('', inplace=True)
        
        # Export to file
        output = BytesIO()
        
        # Add a row number index for easy reference
        df.index = range(1, len(df) + 1)
        df.index.name = 'No.'
        
        if file_format.lower() == "csv":
            df.to_csv(output, index=True)
            output.seek(0)
            media_type = "text/csv"
            filename = f"va_records_export_{date.today().isoformat()}.csv"
        else:  # Default to Excel
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=True, sheet_name="VA Records")
            output.seek(0)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"va_records_export_{date.today().isoformat()}.xlsx"
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        
        return StreamingResponse(
            output,
            media_type=media_type,
            headers=headers
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export VA records: {str(e)}"
        )
