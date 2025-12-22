# Automatizaciones para Dashboards - SIATA

Este repositorio contiene los flujos de trabajo (pipelines) automatizados para la extracción, transformación y carga (ETL) de información geoespacial hacia la suite de **ArcGIS Online** del Proyecto SIATA.

El objetivo principal es mantener actualizadas las capas críticas para la **Gestión del Riesgo de Desastres** en el Valle de Aburrá, asegurando la disponibilidad de datos en tiempo real para la toma de decisiones.

##  Alcance del Proyecto

La automatización cubre la actualización periódica de las siguientes capas estratégicas:

1.  **Movimientos en Masa:** Procesamiento de alertas geológicas diarias.
2.  **Equipo Operacional:** Seguimiento y actualización de la capa de personal operativo SIATA.
3.  **Susceptibilidad a Incendios:** Cálculo y carga de polígonos de probabilidad de incendios forestales.

##  Arquitectura del Proyecto

Este proyecto sigue una arquitectura modular basada en `cookiecutter-modern-datascience`, desacoplando la lógica de negocio de la ejecución:

```text
AUTOMATIZACIONES-SUBIDA-DASHBOARDS/
├── pipelines/           # Lógica de negocio (ETL y Modelamiento)
│   ├── operational/     # ETL para dashboard operacional
│   ├── mov_masa/        # Procesamiento de KMLs y HTML de alertas
│   └── incendios/       # Lógica de cache-busting y carga de incendios
│
├── serve/               # Puntos de entrada (Scripts de ejecución/Orquestación)
│   ├── run_operacional.py
│   ├── run_mov_masa.py
│   └── run_incendios.py
│
├── utils/               # Módulos transversales (Autenticación ArcGIS, Logs)
├── data/                # Almacenamiento temporal de datos (ignorado por git)
└── Pipfile              # Gestión de dependencias y entorno virtual


Mov_Masa_Incendios_Operacional
