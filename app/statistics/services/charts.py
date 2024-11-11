from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values


async def fetch_charts_statistics( current_user: dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None,  date_type:Optional[str]=None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        print("Fetching charts statistics")
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1
        is_adult_field = config.field_mapping.is_adult
        is_child_field = config.field_mapping.is_child
        is_neonate_field = config.field_mapping.is_neonate
        #
        print(date_type)
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = 'submissiondate'
            elif date_type == 'death_date':
                today_field = 'id10023'
            elif date_type == 'interview_date':
                today_field = 'id10012'
            else:
                today_field = config.field_mapping.date 
        else:
            today_field = config.field_mapping.date 

        deceased_gender = config.field_mapping.deceased_gender


        # locationLimitValues =current_user['access_limit']['limit_by'] or None ## [{value: "value", label: "label"}]
        locationKey, locationLimitValues = get_location_limit_values(current_user)

        
        collection = db.collection(db_collections.VA_TABLE)   # Use the actual collection name here
        bind_vars = {}
        filters = []
        ## filter by location limits
        if locationLimitValues and locationKey:
            filters.append(f"doc.{locationKey} IN @locationValues")
            bind_vars["locationValues"] = locationLimitValues
        ##
        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)
            
            print(bind_vars)

        if locations:
            filters.append(f"doc.{region_field} IN @locations")
            bind_vars["locations"] = locations

        filter_query = "FILTER " + " AND ".join(filters) + " " if filters else ""

        combined_query = f"""
            LET monthlySubmissions = (
                FOR doc IN {collection.name}
                {filter_query}
                COLLECT month = DATE_MONTH(DATE_TIMESTAMP(doc.{today_field})), year = DATE_YEAR(DATE_TIMESTAMP(doc.{today_field})) INTO grouped
                LET count = LENGTH(grouped)
                SORT year, month
                RETURN {{ month, year, count }}
            )

            LET distributionByAge = (
                LET ageGroups = [
                    {{ ageGroup: "adult", count: 0 }},
                    {{ ageGroup: "child", count: 0 }},
                    {{ ageGroup: "neonatal", count: 0 }}
                ]

                LET results = (
                   
                    FOR doc IN {collection.name}
                    {filter_query}
    LET ageGroup = FIRST(
        FOR key IN ["adult", "child", "neonatal"]
            FILTER (key == "adult" AND doc.{is_adult_field} == '1') OR 
                   (key == "child" AND doc.{is_child_field} == '1') OR 
                   (key == "neonatal" AND doc.{is_neonate_field} == '1')
            RETURN key
    ) || "unknown"

    COLLECT group = ageGroup WITH COUNT INTO count
    RETURN {{ group, count }}
                )

                RETURN MERGE(
                    FOR ageGroup IN ageGroups
                        LET matched = FIRST(FOR result IN results FILTER result.group == ageGroup.ageGroup RETURN result)
                        RETURN {{ [ageGroup.ageGroup]: matched != null ? matched.count : ageGroup.count }}
                )
            )

            LET genderDistribution = (
                LET genderGroups = [
                    {{ gender: "male", count: 0 }},
                    {{ gender: "female", count: 0 }},
                    {{ gender: "other", count: 0 }}
                ]

                LET genderResults = (
                    FOR doc IN {collection.name}
                    {filter_query}
                    LET gender = FIRST(
                        FOR key IN ["male", "female", "other"]
                            FILTER (key == "male" AND doc.{deceased_gender} == "male") OR 
                                (key == "female" AND doc.{deceased_gender} == "female") OR 
                                (key == "other" AND doc.{deceased_gender} == "other")
                            RETURN key
                    ) || "unknown"

                    COLLECT group = gender WITH COUNT INTO count
                    RETURN {{ group, count }}
                )

                RETURN MERGE(
                    FOR genderGroup IN genderGroups
                        LET matched = FIRST(FOR result IN genderResults FILTER result.group == genderGroup.gender RETURN result)
                        RETURN {{ [genderGroup.gender]: matched != null ? matched.count : genderGroup.count }}
                )
            )

            RETURN {{
                monthly_submissions: monthlySubmissions,
                distribution_by_age: distributionByAge,
                gender_distribution: genderDistribution
            }}
        """
        
        print(combined_query)
        # Execute the combined query
        cursor = db.aql.execute(combined_query, bind_vars=bind_vars,cache=True)
        result = cursor.next()

        monthly_submissions_data = result['monthly_submissions']
        distribution_by_age_data = result['distribution_by_age'][0]
        distribution_by_gender= result['gender_distribution'][0]

        # Structure the combined response
        response_data = {
            "monthly_submissions": monthly_submissions_data,
            "distribution_by_age": {
                "neonates": distribution_by_age_data["neonatal"],
                "children": distribution_by_age_data["child"],
                "adults": distribution_by_age_data["adult"]
           
            },
            'distribution_by_gender':distribution_by_gender
        }

        return ResponseMainModel(
            data=response_data,
            message="Statistics fetched successfully",
            total=None
        )

    except Exception as e:
        print(e)
        return ResponseMainModel(
            data=None,
            message="Failed to fetch statistics",
            error=str(e),
            total=None
        )