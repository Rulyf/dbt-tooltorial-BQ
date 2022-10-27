{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name in ['prod', 'dev'] and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- else -%}
    
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}

        {%- endif -%}
    
    {%- endif -%}

{%- endmacro %}