# Databricks notebook source
pip install jinja2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from jinja2 import Template

parameters = [
    {
        "table": "spotify_faraz.silver.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration"
    },
    {
        "table": "spotify_faraz.silver.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name",
        "condition": "factstream.user_id = dimuser.user_id"
    },
    {
        "table": "spotify_faraz.silver.dimtrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_id, dimtrack.track_name",
        "condition": "factstream.track_id = dimtrack.track_id"
    }
]

# COMMAND ----------

query_text = """
SELECT
    {%- for param in parameters %}
        {%- set cols = param['cols'].split(',') %}
        {%- for col in cols %}
            {{ col.strip() }}
            {# 1. Add comma if NOT the last column in THIS group #}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
        {# 2. Add comma if NOT the last group in the parameters #}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
FROM
    {{ parameters[0]['table'] }} AS {{ parameters[0]['alias'] }}
{%- for param in parameters[1:] %}
LEFT JOIN 
    {{ param['table'] }} AS {{ param['alias'] }}
ON 
    {{ param['condition'] }}
{%- endfor %}
"""

# COMMAND ----------

template = Template(query_text)
query = template.render(parameters=parameters)
print(query)

# COMMAND ----------

df = spark.sql(query)
display(df)