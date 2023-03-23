{% macro apply_calculations(calculations, columns, naming) %}

    {%- for col in columns -%}

        {% set outer_loop = loop %}
        {%- for cal in calculations -%}
            {{ cal }}({{ col }}) as {{ cal }}_{{ naming[outer_loop.index0] }}
            {% if not loop.last %},{% endif %}
        {%- endfor -%}
        {% if not loop.last %},{% endif %}
    {%- endfor -%}

{% endmacro %}

