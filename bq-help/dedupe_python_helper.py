#pip install jinja2
from jinja2 import Template
from typing import Optional, Union, List

QUERY_TEMPLATE = '''
SELECT
{%- if fields is none %}
    *
{%- else %}
  {%- for field in fields %}
    {{ field }} {{- ", " if not loop.last else "" }}
  {%- endfor %}
{%- endif %}
FROM `{{ table_reference }}`
{%- if primary_keys is none %}
QUALIFY ROW_NUMBER() OVER() = 1
{% else %}
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY 
      {%- for primary_key in primary_keys %}
        {{ primary_key }} {{- ", " if not loop.last else "" }}
      {%- endfor %}    
    {%- if ordering_expressions is not none %}
    ORDER BY 
      {%- for ordering_expression in ordering_expressions %}
        {{ ordering_expression }} {{- ", " if not loop.last else "" }}
      {%- endfor %} 
    {%- endif %}
) = 1
{%- endif -%}
; 
'''


def get_deduplication_query(
        table_reference: str,
        fields: Optional[List] = None,
        primary_keys: Optional[Union[str, List]] = None,
        ordering_expressions: Optional[Union[str, List]] = None
    ) -> str:
    """Create a deduplication query for a table given the primary keys and
    the ordering expressions. 

    :param fields: Table reference to deduplicate.
    :param table_reference: Table reference to deduplicate.
        must have a `(<projec_id>)?.<dataset_id>.<table_name>` pattern.
    :param primary_keys: Primary key(s) name of the table if exist. 
        If not specified, all the fields are considered as a primary key.
    :param ordering_expressions: Field(s) to order on. Can be considered as an expression. 
        For instance, you can add " DESC" to the field to invert the order. (ex: "creation_date DESC")
        If not specified, rows are arbitrarily ordered.
    :return: Deduplication query for the table.
    """


    if primary_keys is None:
        # Is not necessary anymore as it will be considered as a primary key
        ordering_expressions = None

    # Create a list if only a string was given        
    to_list_if_str = lambda x: [x] if isinstance(x, str) else x

    primary_keys = to_list_if_str(primary_keys)
    ordering_expressions = to_list_if_str(ordering_expressions)

    # Render the Jinja Template
    params = {
        'table_reference': table_reference,
        'fields': fields,
        'primary_keys': primary_keys,
        'ordering_expressions': ordering_expressions,
    }

    query = Template(QUERY_TEMPLATE).render(**params)

    return query


if __name__ == '__main__':
    query = get_deduplication_query(
        table_reference='bigquery-public-data.austin_waste.waste_and_diversion',
        fields=[
            'load_id', 'report_date', 'load_type', 'load_time', 'load_weight', 'dropoff_site', 'route_type', 'route_number'
        ],
        primary_keys='load_id',
        ordering_expressions=['report_date DESC', 'load_time DESC'],
    )
    print(query)