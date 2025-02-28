{% macro build_ref_function(model) %}

    {%- set ref_dict = {} -%}
    {%- for _ref in model.refs -%}
        {%- set resolved = ref(*_ref) -%}
        {%- do ref_dict.update({_ref | join("."): resolved | string}) -%}
    {%- endfor -%}

def ref(*args,dbt_load_df_function):
    refs = {{ ref_dict | tojson }}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])

{% endmacro %}

{% macro build_source_function(model) %}

    {%- set source_dict = {} -%}
    {%- for _source in model.sources -%}
        {%- set resolved = source(*_source) -%}
        {%- do source_dict.update({_source | join("."): resolved | string}) -%}
    {%- endfor -%}

def source(*args, dbt_load_df_function):
    sources = {{ source_dict | tojson }}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])

{% endmacro %}

{% macro build_config_dict(model) %}
    {%- set config_dict = {} -%}
    {%- for key in model.config.utilized -%}
        {# TODO: weird type testing with enum, would be much easier to write this logic in Python! #}
        {%- if key == 'language' -%}
          {%- set value = 'python' -%}
        {%- endif -%}
        {%- set value = model.config[key] -%}
        {%- do config_dict.update({key: value}) -%}
    {%- endfor -%}
config_dict = {{ config_dict }}
{% endmacro %}

{% macro py_script_postfix(model) %}
# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

{{ build_ref_function(model ) }}
{{ build_source_function(model ) }}
{{ build_config_dict(model) }}

class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key):
        return config_dict.get(key)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = '{{ this.database }}'
    schema = '{{ this.schema }}'
    identifier = '{{ this.identifier }}'
    def __repr__(self):
        return '{{ this }}'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = {{ is_incremental() }}

# COMMAND ----------
{{py_script_comment()}}
{% endmacro %}

{#-- entry point for add instuctions for running compiled_code --#}
{%macro py_script_comment()%}
{%endmacro%}
