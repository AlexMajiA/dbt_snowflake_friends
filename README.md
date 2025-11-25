# Friends Analytics Project

Este proyecto utiliza **dbt**, **Snowflake** y una arquitectura en capas Medallion (Bronze → Silver → Gold) para analizar datos de la serie *Friends*, 
permitiendo la exploración de emociones, interacciones, rendimiento de episodios, y métricas por director.

---

## Estructura del Proyecto

* `models/`

  * `staging/`: Modelos STG de datos RAW normalizados.
  * `intermediate/`: Modelos incrementales (ej. ingestas).
  * `marts/`: Dimensiones y tablas de hechos (modelo en estrella).
* `snapshots/`: Snapshots con control de cambios (SCD2).
* `seeds/`: Datos de referencia (ej. `DIM_DYNAMICS_MAPPING`).
* `macros/`: Macros reutilizables (`calcular_ranking()`).
* `tests/`: Pruebas personalizadas.

---

## Arquitectura de Capas

### Bronze

* Datos RAW cargados a Snowflake desde el dataset de *Friends*.

### Silver

* Modelos STG limpios con IDs MD5, tipos explícitos y nulos gestionados.
* Snapshots con estrategia CHECK (`episode_meta_snapshot`).
* Modelo incremental: `INT_EPISODE_PERFORMANCE`.

### Gold (Marts)

* Modelo en estrella con:

  * `DIM_EPISODE`
  * `DIM_DIRECTOR`
  * `DIM_CHARACTER`, `DIM_SCENE`, `DIM_EMOTION`
  * `FCT_EPISODE_PERFORMANCE`: Desempeño por episodio.
  * `FCT_EMOTIONS`: Emociones por personaje y escena.

---

## Uso del Proyecto

```bash
# Correr todos los modelos
$ dbt run

# Ejecutar todos los tests
$ dbt test
```

---

## Variables y Configuración

Se emplean variables de entorno para adaptar los nombres de base de datos según el entorno:

```yaml
+database: "{{ env_var('DBT_ENVIRONMENTS') }}_SILVER"
+database: "{{ env_var('DBT_ENVIRONMENT') }}_GOLD"
```

---

## Tests Incluidos

* `not_null` y `unique` para claves primarias.
* Integridad entre tablas (tests SQL personalizados).
* Validación de columnas relevantes (`title`, `year`, `duration`, etc.).

---

## Métricas y Casos de Uso

* Rankings de episodios según vistas, votos y puntuación.
* Evaluación de directores por desempeño de sus episodios.
* Emociones detectadas por escena, personaje y línea.
* Evolución temporal de calificaciones y audiencia.

---

## Consideraciones Finales

* Modelo 100% documentado (`schema.yml`).
* Datos limpios, testeados y modelados.
* Listo para ser explotado por BI/SQL o visualizado con herramientas analíticas.

---

## Defensa del Proyecto

- Arquitectura modular y escalable
- Snapshot y modelo incremental funcionales
- Esquema en estrella bien diseñado
- Documentación y tests implementados
- Casos de uso analíticos bien definidos

---

**Autor**: Alejandro Martínez Jiménez
**Tecnologías**: dbt Cloud (Free), Snowflake, Jinja2, SQL


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices