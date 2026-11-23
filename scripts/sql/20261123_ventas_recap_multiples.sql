-- Migración controlada para permitir múltiples filas RECAP en public.ventas.
-- Base prevista: 0106657489001
-- Ejecutar en pgAdmin. No ejecutar COMMIT hasta revisar las consultas finales.
-- Requiere respaldo completo previo de la base.

BEGIN;

LOCK TABLE public.ventas IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF current_database() <> '0106657489001' THEN
        RAISE EXCEPTION
            'Base inesperada: %. Esta migración está preparada para 0106657489001.',
            current_database();
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'ventas'
          AND column_name = 'venta_id'
    ) THEN
        RAISE EXCEPTION 'La columna venta_id ya existe. Revisar el estado de la migración.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.ventas'::regclass
          AND contype = 'p'
          AND conname = 'ventas_pkey'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (numfactur)'
    ) THEN
        RAISE EXCEPTION
            'La clave primaria de ventas no es PRIMARY KEY (numfactur). Migración cancelada.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE confrelid = 'public.ventas'::regclass
          AND contype = 'f'
    ) THEN
        RAISE EXCEPTION
            'Hay claves foráneas de otras tablas hacia ventas. Migración cancelada.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'public.ventas'::regclass
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'Hay triggers en ventas. Migración cancelada.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.ventas
        WHERE BTRIM(numfactur::text) = 'RECAP'
          AND (
              NULLIF(BTRIM(numret::text), '') IS NULL
              OR NULLIF(BTRIM(autret::text), '') IS NULL
          )
    ) THEN
        RAISE EXCEPTION
            'Hay un RECAP sin numret o autret. Corregirlo antes de crear el índice.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.ventas
        WHERE BTRIM(numfactur::text) = 'RECAP'
        GROUP BY BTRIM(numret::text), BTRIM(autret::text)
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Hay retenciones RECAP duplicadas por numret y autret. Migración cancelada.';
    END IF;

    IF to_regclass('public.ventas_backup_pre_recap_20261123') IS NOT NULL THEN
        RAISE EXCEPTION
            'Ya existe ventas_backup_pre_recap_20261123. No se sobrescribirá.';
    END IF;
END
$$;

-- Copia de seguridad adicional de las filas originales de ventas.
CREATE TABLE public.ventas_backup_pre_recap_20261123 AS
SELECT * FROM public.ventas;

-- Identificador interno para todas las filas, incluidas las ya existentes.
ALTER TABLE public.ventas
    ADD COLUMN venta_id BIGINT GENERATED ALWAYS AS IDENTITY;

-- Cambiar la PK sin modificar numfactur ni el resto de los datos.
ALTER TABLE public.ventas
    DROP CONSTRAINT ventas_pkey;

ALTER TABLE public.ventas
    ADD CONSTRAINT ventas_pkey PRIMARY KEY (venta_id);

-- Las facturas normales conservan numfactur único.
-- Los valores RECAP quedan excluidos de este índice.
CREATE UNIQUE INDEX ux_ventas_numfactur_no_recap
    ON public.ventas (numfactur)
    WHERE BTRIM(numfactur::text) <> 'RECAP';

-- Cada retención RECAP queda identificada por su número y autorización.
CREATE UNIQUE INDEX ux_ventas_recap_numret_autret
    ON public.ventas (BTRIM(numret::text), BTRIM(autret::text))
    WHERE BTRIM(numfactur::text) = 'RECAP';

-- Verificaciones: revisar los resultados antes de ejecutar COMMIT.
SELECT
    (SELECT COUNT(*) FROM public.ventas_backup_pre_recap_20261123)
        AS filas_respaldo,
    (SELECT COUNT(*) FROM public.ventas)
        AS filas_actuales,
    (SELECT COUNT(*) FROM public.ventas WHERE venta_id IS NULL)
        AS ids_nulos,
    (SELECT COUNT(DISTINCT venta_id) FROM public.ventas)
        AS ids_distintos;

SELECT
    venta_id,
    numfactur,
    numret,
    autret,
    fecfactur,
    ruccedcli,
    nomcli
FROM public.ventas
WHERE BTRIM(numfactur::text) = 'RECAP'
ORDER BY venta_id;

SELECT
    conname,
    pg_get_constraintdef(oid) AS definicion
FROM pg_constraint
WHERE conrelid = 'public.ventas'::regclass;

SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'ventas'
ORDER BY indexname;

-- Si todo está correcto, ejecutar COMMIT manualmente.
-- Si algo no coincide, ejecutar ROLLBACK.
