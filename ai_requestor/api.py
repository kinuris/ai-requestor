import frappe
import requests
import datetime
import json
import os

from frappe.utils.response import Response

def serialize_dict(data):
    if isinstance(data, dict):
        return {key: serialize_dict(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_dict(item) for item in data]
    else:
        return data

def recursive_serialize(data):
    return json.dumps(serialize_dict(data))

def get_tables():
    return frappe.db.sql("""
        SELECT name 
        FROM `tabDocType`
        WHERE issingle = 0 
        AND istable = 0
        AND is_virtual = 0
        AND custom = 0
        AND module != 'Core'
        AND module != 'Website'
        AND module != 'Workflow'
        AND module != 'Email'
        AND module != 'Custom'
        AND module != 'Geo'
        AND module != 'Desk'
        AND module != 'Printing'
        AND module != 'Data Migration'
        AND module != 'Automation'
        AND module != 'Social'
        AND module != 'Authentication'
        AND module != 'File'
        AND module != 'Integrations'
        AND module != 'Contacts'
    """, as_dict=True)

def get_schema():
    # Dictionary to store table schemas
    schema = {}
    
    # Set of default Frappe system fields to exclude
    default_fields = {
        'owner', 'creation', 'modified', 'modified_by', 
        'parent', 'parentfield', 'parenttype', 'idx', 'docstatus',
        '_user_tags', '_comments', '_assign', '_liked_by'
    }

    # Get list of tables from database
    tables = get_tables() 
    
    # Iterate through tables to build schema
    for table in tables:
        table_name = f"tab{table['name']}"

        # Get column info for current table
        columns = frappe.db.sql(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = DATABASE()
        """, as_dict=True)
        
        # Filter out default system fields
        filtered_columns = {
            col['column_name']: col['data_type'] 
            for col in columns 
            if col['column_name'] not in default_fields
        }
        
        # Only include tables with non-default fields
        if filtered_columns:
            schema[table_name] = filtered_columns
    
    return schema

@frappe.whitelist(allow_guest=True)
def ai_suggest():
    if frappe.request.method != "POST":
        frappe.throw('Only POST requests are allowed')

    schema = get_schema()

    # Remove 'tab' prefix from table names
    schema = {k.removeprefix('tab'): v for k, v in schema.items()}
    data = {
        "schema": schema
    }

    local_server_url = os.getenv('LOCAL_SERVER_URL')
    response = requests.post(f'{local_server_url}/api/ai-suggest', json=data, stream=True)
    def stream_generator():
        for chunk in response.iter_content(chunk_size=None):
            yield chunk

    return Response(stream_generator(), content_type='application/json') 

@frappe.whitelist(allow_guest=True)
def ai_query():
    """
    Retrieves schema and data from database tables, excluding default system fields.
    Returns a structured response containing table schemas and their data.
    """

    if frappe.request.method != "POST":
        frappe.throw('Only POST requests are allowed')

    # Get and validate request data
    request_data = frappe.request.get_json()
    if not request_data or 'prompt' not in request_data:
        frappe.throw('Missing ["prompt"] in request body')

    # Get schema once and reuse
    schema = get_schema()
    
    # Build data for all tables in one pass
    body = []
    for table, fields in schema.items():
        # Get all records for the table
        if fields:
            # Get all records first
            records = frappe.db.sql(
                f"SELECT {', '.join(f'`{field}`' for field in fields)} FROM `{table}`", 
                as_dict=True
            )
            
            # Filter out null values for each record
            filtered_records = []
            for record in records:
                filtered_record = {}
                for k, v in record.items():
                    if v is not None:
                        if isinstance(v, (datetime.date, datetime.datetime)):
                            filtered_record[k] = v.isoformat()
                        else:
                            filtered_record[k] = v
                if filtered_record:  # Only include if there are non-null values
                    filtered_records.append(filtered_record)
            
            if filtered_records:  # Only include table if it has records with non-null values
                body.append({
                    'table': table.removeprefix('tab'),
                    'data': filtered_records
                })

    local_server_url = os.getenv('LOCAL_SERVER_URL')
    data = {
        'prompt': request_data['prompt'],
        'data': body
    }

    # Peek mode returns the data without sending it to the AI server
    if request_data.get('peek', False):
        return data

    response = requests.post(f'{local_server_url}/api/ai-query', json=data, stream=True)
    def stream_generator():
        for chunk in response.iter_content(chunk_size=None):
            yield chunk 

    return Response(stream_generator(), content_type='application/json') 
    
@frappe.whitelist(allow_guest=True)
def stream_numbers():
    pass