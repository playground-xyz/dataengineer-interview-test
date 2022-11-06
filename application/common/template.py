
class Template:

    INSERT_CAT_FACT = """
            INSERT INTO "CAT_FACTS" ("FACT_PK_KEY", "FACT")
            VALUES 
            {% for key, value in data.items() -%}
            ("{{ key }}", "{{ value }}"){{ ",
             " if not loop.last else "" }}
             {%- endfor %}
    """

    INSERT_CAT_WORD_UNICODE_COUNT = """
            INSERT INTO "{{table_name}}" ("FACT_FK_KEY", "{{entity_name}}", "{{entity_count}}")
            VALUES 
            {% for hashkey, value in data.items() -%}
            ("{{ hahskey }}", {% for word, count in value.items() %} 
	    			   "{{word}}", {{count}}
				   {% endfor %}
	    {{ ",
             " if not loop.last else "" }}
             {%- endfor %}
	     """
