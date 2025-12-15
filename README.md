# Friends Analytics · Data Engineering Project

Proyecto de **Data Engineering** construido con **dbt** y **Snowflake**, siguiendo una arquitectura en capas **Medallion (Bronze → Silver → Gold)** para el análisis analítico de datos de la serie *Friends*.

El objetivo del proyecto es diseñar un pipeline ELT escalable que permita explotar métricas analíticas relacionadas con episodios, personajes, emociones e interacción entre entidades.

---

## Arquitectura del Proyecto

El pipeline sigue una arquitectura Medallion claramente definida:

### Bronze
- Ingesta de datos RAW del dataset de *Friends* en Snowflake.
- Conservación de los datos originales sin transformaciones.

### Silver
- Normalización y limpieza de datos mediante modelos **staging**.
- Generación de identificadores MD5.
- Tipado explícito de columnas y gestión de valores nulos.
- Implementación de **snapshots SCD Type 2** para control de cambios.
- Modelos incrementales para optimizar la carga de datos.

### Gold (Analytics / Marts)
- Modelado dimensional en **esquema en estrella**.
- Tablas de hechos y dimensiones preparadas para consumo analítico y BI.

---

## Estructura de Directorios

models/
├── staging/ # Normalización de datos RAW
├── intermediate/ # Transformaciones e ingestas incrementales
└── marts/ # Dimensiones y tablas de hechos
snapshots/ # Control de cambios (SCD2)
seeds/ # Datos de referencia
macros/ # Macros reutilizables
tests/ # Tests personalizados

---

## Modelado Analítico

### Dimensiones
- `DIM_EPISODE`
- `DIM_DIRECTOR`
- `DIM_CHARACTER`
- `DIM_SCENE`
- `DIM_EMOTION`

### Tablas de Hechos
- `FCT_EPISODE_PERFORMANCE`  
  Métricas de rendimiento por episodio (audiencia, votos, rating).
- `FCT_EMOTIONS`  
  Emociones detectadas por personaje y escena.

---

## Calidad de Datos y Testing

El proyecto incorpora validaciones automáticas mediante **dbt tests**:

- Tests `not_null` y `unique` en claves primarias.
- Tests de integridad referencial entre hechos y dimensiones.
- Tests SQL personalizados para validación de reglas de negocio.
- Documentación completa mediante `schema.yml`.

---

## Ejecución del Proyecto

```bash
# Ejecutar modelos
dbt run

# Ejecutar tests
dbt test


+database: "{{ env_var('DBT_ENVIRONMENT') }}_SILVER"
+database: "{{ env_var('DBT_ENVIRONMENT') }}_GOLD"
