
from jinja2 import Template


class SqlTemplate:
    def get_number_of_words_sql():
        sql = """
                SELECT
                        COUNT(DISTINCT word) AS num_of_word
                FROM CAT_FACT_WORD_COUNT
                """
        return sql

    def get_nth_common_or_least_unicode(top_nth_code: int = 1, most_command: bool = True):
        sql = """
           WITH sum_unicode AS (
             SELECT
                    unicode
                   ,SUM(unicode_count) AS unicode_count
             FROM
                CAT_FACT_UNICODE_COUNT
              GROUP BY unicode)
           , sum_rank_unicode AS (
           SELECT
                unicode
                ,unicode_count
                ,DENSE_RANK() OVER (ORDER BY unicode_count {% if most_command %} DESC {% endif %}) as _rank
           FROM sum_unicode
           )

           SELECT unicode, unicode_count
           FROM sum_rank_unicode
           WHERE _rank = {{top_nth_code}}

        """
        render_sql = Template(sql).render(
            top_nth_code=top_nth_code, most_command=most_command)

        return render_sql

    def get_top_bottom_n_words(top_nth_word: int = 20, most_command: bool = True):
        sql = """
           WITH sum_word_count AS (
             SELECT
                    word
                   ,SUM(word_count) AS word_count
             FROM
                CAT_FACT_WORD_COUNT
              GROUP BY word)
           , sum_rank_word AS (
           SELECT
                word
                ,word_count
                ,RANK() OVER (ORDER BY word_count {% if most_command %} DESC {% endif %}) as _rank
           FROM sum_word_count
           )

           SELECT word, word_count
           FROM sum_rank_word
           WHERE _rank <= {{top_nth_word}}
           ORDER BY 2 DESC

        """
        render_sql = Template(sql).render(
            top_nth_word=top_nth_word, most_command=most_command)

        return render_sql

    def get_nth_common_or_least_country(top_nth_country: int = 1, most_command: bool = True):
        sql = """
           WITH sum_word_count AS (
             SELECT
                    word
                   ,SUM(word_count) AS word_count
             FROM
                CAT_FACT_WORD_COUNT
             JOIN 
                COUNTRIES
             ON CAT_FACT_WORD_COUNT.word = COUNTRIES.country_name
              GROUP BY word)
           , sum_rank_word AS (
           SELECT
                word
                ,word_count
                ,DENSE_RANK() OVER (ORDER BY word_count {% if most_command %} DESC {% endif %}) as _rank
           FROM sum_word_count
           )

           SELECT word, word_count
           FROM sum_rank_word
           WHERE _rank <= {{top_nth_country}}

        """
        render_sql = Template(sql).render(
            top_nth_country=top_nth_country, most_command=most_command)

        return render_sql