{% macro calcular_ranking(metric) %}
case 
    when {{ metric }} is null then null
    else dense_rank() over (order by {{ metric }} desc)
end
{% endmacro %}





--NOTAS:
--Implementé una macro en dbt llamada calcular_ranking() que permite reutilizar
--la lógica de ranking en distintas métricas (votes, stars, views).
--La macro recibe el nombre de la métrica como parámetro y genera el SQL
--correspondiente usando Jinja. Esto evita duplicar código y hace que la fact table
--sea más limpia y configurable.”
--dense_rank para que los empates no salten posiciones.