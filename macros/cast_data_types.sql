{% macro cast_data_types(columns, data_type) %}

    {%- for column in columns -%}
        
        {{ column }}::{{ data_type }} as {{ column }}
        {% if not loop.last %},{% endif %}

    {%- endfor -%}

{% endmacro %}
