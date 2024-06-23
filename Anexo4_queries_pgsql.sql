SELECT * FROM public."OFF_Viz_ini"


-- Listar columnas en orden
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'  
  AND table_name   = 'OFF_Viz_ini'
ORDER BY ordinal_position;


-- Eliminar filas duplicadas
WITH duplicados AS (
    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY "code","created_datetime","product_name","generic_name","quantity","packaging_en","brands_tags","categories_en","origins_en","manufacturing_places_tags","labels_en","emb_codes_tags","first_packaging_code_geo","cities_tags","purchase_places","stores","countries_en","ingredients_tags","ingredients_analysis_tags","allergens_en","traces_en","serving_size","serving_quantity","no_nutrition_data","additives_en","nutriscore_score","nutriscore_grade","nova_group","pnns_groups_1","pnns_groups_2","food_groups_en","states_en","brand_owner","ecoscore_score","ecoscore_grade","nutrient_levels_tags","product_quantity","owner","data_quality_errors_tags","unique_scans_n","popularity_tags","completeness","main_category_en","energy-kj_100g","energy-kcal_100g","energy_100g","energy-from-fat_100g","fat_100g","saturated-fat_100g","butyric-acid_100g","caproic-acid_100g","caprylic-acid_100g","capric-acid_100g","lauric-acid_100g","myristic-acid_100g","palmitic-acid_100g","stearic-acid_100g","arachidic-acid_100g","behenic-acid_100g","lignoceric-acid_100g","cerotic-acid_100g","montanic-acid_100g","melissic-acid_100g","unsaturated-fat_100g","monounsaturated-fat_100g","omega-9-fat_100g","polyunsaturated-fat_100g","omega-3-fat_100g","omega-6-fat_100g","alpha-linolenic-acid_100g","eicosapentaenoic-acid_100g","docosahexaenoic-acid_100g","linoleic-acid_100g","arachidonic-acid_100g","gamma-linolenic-acid_100g","dihomo-gamma-linolenic-acid_100g","oleic-acid_100g","elaidic-acid_100g","gondoic-acid_100g","mead-acid_100g","erucic-acid_100g","nervonic-acid_100g","trans-fat_100g","cholesterol_100g","carbohydrates_100g","sugars_100g","added-sugars_100g","sucrose_100g","glucose_100g","fructose_100g","lactose_100g","maltose_100g","maltodextrins_100g","starch_100g","polyols_100g","erythritol_100g","fiber_100g","soluble-fiber_100g","insoluble-fiber_100g","proteins_100g","casein_100g","serum-proteins_100g","nucleotides_100g","salt_100g","added-salt_100g","sodium_100g","alcohol_100g","vitamin-a_100g","beta-carotene_100g","vitamin-d_100g","vitamin-e_100g","vitamin-k_100g","vitamin-c_100g","vitamin-b1_100g","vitamin-b2_100g","vitamin-pp_100g","vitamin-b6_100g","vitamin-b9_100g","folates_100g","vitamin-b12_100g","biotin_100g","pantothenic-acid_100g","silica_100g","bicarbonate_100g","potassium_100g","chloride_100g","calcium_100g","phosphorus_100g","iron_100g","magnesium_100g","zinc_100g","copper_100g","manganese_100g","fluoride_100g","selenium_100g","chromium_100g","molybdenum_100g","iodine_100g","caffeine_100g","taurine_100g","ph_100g","fruits-vegetables-nuts_100g","fruits-vegetables-nuts-dried_100g","fruits-vegetables-nuts-estimate_100g","fruits-vegetables-nuts-estimate-from-ingredients_100g","collagen-meat-protein-ratio_100g","cocoa_100g","chlorophyl_100g","carbon-footprint_100g","carbon-footprint-from-meat-or-fish_100g","nutrition-score-fr_100g","nutrition-score-uk_100g","glycemic-index_100g","water-hardness_100g","choline_100g","phylloquinone_100g","beta-glucan_100g","inositol_100g","carnitine_100g","sulphate_100g","nitrate_100g","acidity_100g" 
	ORDER BY ctid) as row_num
    FROM public."OFF_Viz_ini"
)
DELETE FROM public."OFF_Viz_ini"
WHERE ctid IN (
    SELECT ctid FROM duplicados WHERE row_num > 1
);
-- DELETE 21

-- Contar filas con valor no nulo en created_datetime y sin formato ISO 8601
SELECT COUNT(*)
FROM public."OFF_Viz_ini"
WHERE created_datetime IS NOT NULL AND created_datetime NOT LIKE '____-__-__T__:__:__Z';
-- 2

-- Transformar a NULL filas con valor no nulo en created_datetime y sin formato ISO 8601
UPDATE public."OFF_Viz_ini"
SET created_datetime = NULL
WHERE created_datetime IS NOT NULL AND created_datetime NOT LIKE '____-__-__T__:__:__Z';
-- UPDATE 2

-- Cambiar fechas a tipo de datos estándar TIMESTAMP
ALTER TABLE public."OFF_Viz_ini"
ALTER COLUMN created_datetime
TYPE timestamptz USING TO_TIMESTAMP(created_datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
--ALTER TABLE

-- Ver rangos de fechas del dataset
SELECT
    min(created_datetime),
    max(created_datetime),
    count(*) filter (where created_datetime is null)
   FROM
    public."OFF_Viz_ini";
-- 2012-01-31 14:43:58+01 -- 2024-05-12 05:50:00+02

-- Listar años y cuenta de filas por cada año
SELECT extract(year from created_datetime), count(*)
     FROM public."OFF_Viz_ini"
     GROUP BY 1 ORDER BY 1 DESC;
-- extract,"count"
-- 2024,"199462"
-- 2023,"361759"
-- 2022,"600940"
-- 2021,"517386"
-- 2020,"469089"
-- 2019,"369397"
-- 2018,"325344"
-- 2017,"284024"
-- 2016,"46102"
-- 2015,"35182"
-- 2014,"13388"
-- 2013,"10065"
-- 2012,"4467"


-- Detectar rows cuasi-duplicadas (con una diferencia de 5 o menos columnas)--> muy costoso a nivel tiempo-recurso al comparar pares de filas con dataset enorme
WITH numeradas AS (
    SELECT
        *,
        row_number() OVER () AS fila
    FROM
        public."OFF_Viz_ini"
),
comparaciones AS (
    SELECT
        a.fila AS fila_a,
        b.fila AS fila_b,
        (a."code" IS DISTINCT FROM b."code")::int +
        (a."created_datetime" IS DISTINCT FROM b."created_datetime")::int +
        (a."product_name" IS DISTINCT FROM b."product_name")::int +
        (a."generic_name" IS DISTINCT FROM b."generic_name")::int +
        (a."quantity" IS DISTINCT FROM b."quantity")::int +
        (a."packaging_en" IS DISTINCT FROM b."packaging_en")::int +
        (a."brands_tags" IS DISTINCT FROM b."brands_tags")::int +
        (a."categories_en" IS DISTINCT FROM b."categories_en")::int +
        (a."origins_en" IS DISTINCT FROM b."origins_en")::int +
        (a."manufacturing_places_tags" IS DISTINCT FROM b."manufacturing_places_tags")::int +
        (a."labels_en" IS DISTINCT FROM b."labels_en")::int +
        (a."emb_codes_tags" IS DISTINCT FROM b."emb_codes_tags")::int +
        (a."first_packaging_code_geo" IS DISTINCT FROM b."first_packaging_code_geo")::int +
        (a."cities_tags" IS DISTINCT FROM b."cities_tags")::int +
        (a."purchase_places" IS DISTINCT FROM b."purchase_places")::int +
        (a."stores" IS DISTINCT FROM b."stores")::int +
        (a."countries_en" IS DISTINCT FROM b."countries_en")::int +
        (a."ingredients_tags" IS DISTINCT FROM b."ingredients_tags")::int +
        (a."ingredients_analysis_tags" IS DISTINCT FROM b."ingredients_analysis_tags")::int +
        (a."allergens_en" IS DISTINCT FROM b."allergens_en")::int +
        (a."traces_en" IS DISTINCT FROM b."traces_en")::int +
        (a."serving_size" IS DISTINCT FROM b."serving_size")::int +
        (a."serving_quantity" IS DISTINCT FROM b."serving_quantity")::int +
        (a."no_nutrition_data" IS DISTINCT FROM b."no_nutrition_data")::int +
        (a."additives_en" IS DISTINCT FROM b."additives_en")::int +
        (a."nutriscore_score" IS DISTINCT FROM b."nutriscore_score")::int +
        (a."nutriscore_grade" IS DISTINCT FROM b."nutriscore_grade")::int +
        (a."nova_group" IS DISTINCT FROM b."nova_group")::int +
        (a."pnns_groups_1" IS DISTINCT FROM b."pnns_groups_1")::int +
        (a."pnns_groups_2" IS DISTINCT FROM b."pnns_groups_2")::int +
        (a."food_groups_en" IS DISTINCT FROM b."food_groups_en")::int +
        (a."states_en" IS DISTINCT FROM b."states_en")::int +
        (a."brand_owner" IS DISTINCT FROM b."brand_owner")::int +
        (a."ecoscore_score" IS DISTINCT FROM b."ecoscore_score")::int +
        (a."ecoscore_grade" IS DISTINCT FROM b."ecoscore_grade")::int +
        (a."nutrient_levels_tags" IS DISTINCT FROM b."nutrient_levels_tags")::int +
        (a."product_quantity" IS DISTINCT FROM b."product_quantity")::int +
        (a."owner" IS DISTINCT FROM b."owner")::int +
        (a."data_quality_errors_tags" IS DISTINCT FROM b."data_quality_errors_tags")::int +
        (a."unique_scans_n" IS DISTINCT FROM b."unique_scans_n")::int +
        (a."popularity_tags" IS DISTINCT FROM b."popularity_tags")::int +
        (a."completeness" IS DISTINCT FROM b."completeness")::int +
        (a."main_category_en" IS DISTINCT FROM b."main_category_en")::int +
        (a."energy-kj_100g" IS DISTINCT FROM b."energy-kj_100g")::int +
        (a."energy-kcal_100g" IS DISTINCT FROM b."energy-kcal_100g")::int +
        (a."energy_100g" IS DISTINCT FROM b."energy_100g")::int +
        (a."energy-from-fat_100g" IS DISTINCT FROM b."energy-from-fat_100g")::int +
        (a."fat_100g" IS DISTINCT FROM b."fat_100g")::int +
        (a."saturated-fat_100g" IS DISTINCT FROM b."saturated-fat_100g")::int +
        (a."butyric-acid_100g" IS DISTINCT FROM b."butyric-acid_100g")::int +
        (a."caproic-acid_100g" IS DISTINCT FROM b."caproic-acid_100g")::int +
        (a."caprylic-acid_100g" IS DISTINCT FROM b."caprylic-acid_100g")::int +
        (a."capric-acid_100g" IS DISTINCT FROM b."capric-acid_100g")::int +
        (a."lauric-acid_100g" IS DISTINCT FROM b."lauric-acid_100g")::int +
        (a."myristic-acid_100g" IS DISTINCT FROM b."myristic-acid_100g")::int +
        (a."palmitic-acid_100g" IS DISTINCT FROM b."palmitic-acid_100g")::int +
        (a."stearic-acid_100g" IS DISTINCT FROM b."stearic-acid_100g")::int +
        (a."arachidic-acid_100g" IS DISTINCT FROM b."arachidic-acid_100g")::int +
        (a."behenic-acid_100g" IS DISTINCT FROM b."behenic-acid_100g")::int +
        (a."lignoceric-acid_100g" IS DISTINCT FROM b."lignoceric-acid_100g")::int +
        (a."cerotic-acid_100g" IS DISTINCT FROM b."cerotic-acid_100g")::int +
        (a."montanic-acid_100g" IS DISTINCT FROM b."montanic-acid_100g")::int +
        (a."melissic-acid_100g" IS DISTINCT FROM b."melissic-acid_100g")::int +
        (a."unsaturated-fat_100g" IS DISTINCT FROM b."unsaturated-fat_100g")::int +
        (a."monounsaturated-fat_100g" IS DISTINCT FROM b."monounsaturated-fat_100g")::int +
        (a."omega-9-fat_100g" IS DISTINCT FROM b."omega-9-fat_100g")::int +
        (a."polyunsaturated-fat_100g" IS DISTINCT FROM b."polyunsaturated-fat_100g")::int +
        (a."omega-3-fat_100g" IS DISTINCT FROM b."omega-3-fat_100g")::int +
        (a."omega-6-fat_100g" IS DISTINCT FROM b."omega-6-fat_100g")::int +
        (a."alpha-linolenic-acid_100g" IS DISTINCT FROM b."alpha-linolenic-acid_100g")::int +
        (a."eicosapentaenoic-acid_100g" IS DISTINCT FROM b."eicosapentaenoic-acid_100g")::int +
        (a."docosahexaenoic-acid_100g" IS DISTINCT FROM b."docosahexaenoic-acid_100g")::int +
        (a."linoleic-acid_100g" IS DISTINCT FROM b."linoleic-acid_100g")::int +
        (a."arachidonic-acid_100g" IS DISTINCT FROM b."arachidonic-acid_100g")::int +
        (a."gamma-linolenic-acid_100g" IS DISTINCT FROM b."gamma-linolenic-acid_100g")::int +
        (a."dihomo-gamma-linolenic-acid_100g" IS DISTINCT FROM b."dihomo-gamma-linolenic-acid_100g")::int +
        (a."oleic-acid_100g" IS DISTINCT FROM b."oleic-acid_100g")::int +
        (a."elaidic-acid_100g" IS DISTINCT FROM b."elaidic-acid_100g")::int +
        (a."gondoic-acid_100g" IS DISTINCT FROM b."gondoic-acid_100g")::int +
        (a."mead-acid_100g" IS DISTINCT FROM b."mead-acid_100g")::int +
        (a."erucic-acid_100g" IS DISTINCT FROM b."erucic-acid_100g")::int +
        (a."nervonic-acid_100g" IS DISTINCT FROM b."nervonic-acid_100g")::int +
        (a."trans-fat_100g" IS DISTINCT FROM b."trans-fat_100g")::int +
        (a."cholesterol_100g" IS DISTINCT FROM b."cholesterol_100g")::int +
        (a."carbohydrates_100g" IS DISTINCT FROM b."carbohydrates_100g")::int +
        (a."sugars_100g" IS DISTINCT FROM b."sugars_100g")::int +
        (a."added-sugars_100g" IS DISTINCT FROM b."added-sugars_100g")::int +
        (a."sucrose_100g" IS DISTINCT FROM b."sucrose_100g")::int +
        (a."glucose_100g" IS DISTINCT FROM b."glucose_100g")::int +
        (a."fructose_100g" IS DISTINCT FROM b."fructose_100g")::int +
        (a."lactose_100g" IS DISTINCT FROM b."lactose_100g")::int +
        (a."maltose_100g" IS DISTINCT FROM b."maltose_100g")::int +
        (a."maltodextrins_100g" IS DISTINCT FROM b."maltodextrins_100g")::int +
        (a."starch_100g" IS DISTINCT FROM b."starch_100g")::int +
        (a."polyols_100g" IS DISTINCT FROM b."polyols_100g")::int +
        (a."erythritol_100g" IS DISTINCT FROM b."erythritol_100g")::int +
        (a."fiber_100g" IS DISTINCT FROM b."fiber_100g")::int +
        (a."soluble-fiber_100g" IS DISTINCT FROM b."soluble-fiber_100g")::int +
        (a."insoluble-fiber_100g" IS DISTINCT FROM b."insoluble-fiber_100g")::int +
        (a."proteins_100g" IS DISTINCT FROM b."proteins_100g")::int +
        (a."casein_100g" IS DISTINCT FROM b."casein_100g")::int +
        (a."serum-proteins_100g" IS DISTINCT FROM b."serum-proteins_100g")::int +
        (a."nucleotides_100g" IS DISTINCT FROM b."nucleotides_100g")::int +
        (a."salt_100g" IS DISTINCT FROM b."salt_100g")::int +
        (a."added-salt_100g" IS DISTINCT FROM b."added-salt_100g")::int +
        (a."sodium_100g" IS DISTINCT FROM b."sodium_100g")::int +
        (a."alcohol_100g" IS DISTINCT FROM b."alcohol_100g")::int +
        (a."vitamin-a_100g" IS DISTINCT FROM b."vitamin-a_100g")::int +
        (a."beta-carotene_100g" IS DISTINCT FROM b."beta-carotene_100g")::int +
        (a."vitamin-d_100g" IS DISTINCT FROM b."vitamin-d_100g")::int +
        (a."vitamin-e_100g" IS DISTINCT FROM b."vitamin-e_100g")::int +
        (a."vitamin-k_100g" IS DISTINCT FROM b."vitamin-k_100g")::int +
        (a."vitamin-c_100g" IS DISTINCT FROM b."vitamin-c_100g")::int +
        (a."vitamin-b1_100g" IS DISTINCT FROM b."vitamin-b1_100g")::int +
        (a."vitamin-b2_100g" IS DISTINCT FROM b."vitamin-b2_100g")::int +
        (a."vitamin-pp_100g" IS DISTINCT FROM b."vitamin-pp_100g")::int +
        (a."vitamin-b6_100g" IS DISTINCT FROM b."vitamin-b6_100g")::int +
        (a."vitamin-b9_100g" IS DISTINCT FROM b."vitamin-b9_100g")::int +
        (a."folates_100g" IS DISTINCT FROM b."folates_100g")::int +
        (a."vitamin-b12_100g" IS DISTINCT FROM b."vitamin-b12_100g")::int +
        (a."biotin_100g" IS DISTINCT FROM b."biotin_100g")::int +
        (a."pantothenic-acid_100g" IS DISTINCT FROM b."pantothenic-acid_100g")::int +
        (a."silica_100g" IS DISTINCT FROM b."silica_100g")::int +
        (a."bicarbonate_100g" IS DISTINCT FROM b."bicarbonate_100g")::int +
        (a."potassium_100g" IS DISTINCT FROM b."potassium_100g")::int +
        (a."chloride_100g" IS DISTINCT FROM b."chloride_100g")::int +
        (a."calcium_100g" IS DISTINCT FROM b."calcium_100g")::int +
        (a."phosphorus_100g" IS DISTINCT FROM b."phosphorus_100g")::int +
        (a."iron_100g" IS DISTINCT FROM b."iron_100g")::int +
        (a."magnesium_100g" IS DISTINCT FROM b."magnesium_100g")::int +
        (a."zinc_100g" IS DISTINCT FROM b."zinc_100g")::int +
        (a."copper_100g" IS DISTINCT FROM b."copper_100g")::int +
        (a."manganese_100g" IS DISTINCT FROM b."manganese_100g")::int +
        (a."fluoride_100g" IS DISTINCT FROM b."fluoride_100g")::int +
        (a."selenium_100g" IS DISTINCT FROM b."selenium_100g")::int +
        (a."chromium_100g" IS DISTINCT FROM b."chromium_100g")::int +
        (a."molybdenum_100g" IS DISTINCT FROM b."molybdenum_100g")::int +
        (a."iodine_100g" IS DISTINCT FROM b."iodine_100g")::int +
        (a."caffeine_100g" IS DISTINCT FROM b."caffeine_100g")::int +
        (a."taurine_100g" IS DISTINCT FROM b."taurine_100g")::int +
        (a."ph_100g" IS DISTINCT FROM b."ph_100g")::int +
        (a."fruits-vegetables-nuts_100g" IS DISTINCT FROM b."fruits-vegetables-nuts_100g")::int +
        (a."fruits-vegetables-nuts-dried_100g" IS DISTINCT FROM b."fruits-vegetables-nuts-dried_100g")::int +
        (a."fruits-vegetables-nuts-estimate_100g" IS DISTINCT FROM b."fruits-vegetables-nuts-estimate_100g")::int +
        (a."fruits-vegetables-nuts-estimate-from-ingredients_100g" IS DISTINCT FROM b."fruits-vegetables-nuts-estimate-from-ingredients_100g")::int +
        (a."collagen-meat-protein-ratio_100g" IS DISTINCT FROM b."collagen-meat-protein-ratio_100g")::int +
        (a."cocoa_100g" IS DISTINCT FROM b."cocoa_100g")::int +
        (a."chlorophyl_100g" IS DISTINCT FROM b."chlorophyl_100g")::int +
        (a."carbon-footprint_100g" IS DISTINCT FROM b."carbon-footprint_100g")::int +
        (a."carbon-footprint-from-meat-or-fish_100g" IS DISTINCT FROM b."carbon-footprint-from-meat-or-fish_100g")::int +
        (a."nutrition-score-fr_100g" IS DISTINCT FROM b."nutrition-score-fr_100g")::int +
        (a."nutrition-score-uk_100g" IS DISTINCT FROM b."nutrition-score-uk_100g")::int +
        (a."glycemic-index_100g" IS DISTINCT FROM b."glycemic-index_100g")::int +
        (a."water-hardness_100g" IS DISTINCT FROM b."water-hardness_100g")::int +
        (a."choline_100g" IS DISTINCT FROM b."choline_100g")::int +
        (a."phylloquinone_100g" IS DISTINCT FROM b."phylloquinone_100g")::int +
        (a."beta-glucan_100g" IS DISTINCT FROM b."beta-glucan_100g")::int +
        (a."inositol_100g" IS DISTINCT FROM b."inositol_100g")::int +
        (a."carnitine_100g" IS DISTINCT FROM b."carnitine_100g")::int +
        (a."sulphate_100g" IS DISTINCT FROM b."sulphate_100g")::int +
        (a."nitrate_100g" IS DISTINCT FROM b."nitrate_100g")::int +
        (a."acidity_100g" IS DISTINCT FROM b."acidity_100g")::int AS diferencias
    FROM
        numeradas a
    JOIN
        numeradas b
    ON
        a.fila < b.fila
),
cuasi_duplicados AS (
    SELECT
        fila_a,
        fila_b
    FROM
        comparaciones
    WHERE
        diferencias <= 5
)
SELECT
    COUNT(*)
FROM
    cuasi_duplicados;





-- Paso 1: Eliminar las filas donde "created_datetime" sea NULL o "code" no pueda ser convertido a float
-- Paso 2: Modificar el tipo de la columna "code" a float
DELETE FROM public."OFF_Viz_ini"
WHERE 
    created_datetime IS NULL OR 
    NOT (code::text ~ '^-?[0-9]*\.?[0-9]+$');
ALTER TABLE public."OFF_Viz_ini"
ALTER COLUMN code TYPE float USING code::float;

-- Total filas
SELECT COUNT(*) AS total_filas
FROM public."OFF_Viz_ini";
--3211062


-- Detectar duplicados en code (descubrir si se puede utilizar como id único de row)
SELECT code, COUNT(*)
FROM public."OFF_Viz_ini"
GROUP BY code
HAVING COUNT(*) > 1;


SELECT COUNT(*) AS distinct_repeated_codes
FROM (
    SELECT code
    FROM public."OFF_Viz_ini"
    GROUP BY code
    HAVING COUNT(*) > 1
) AS subquery;

-- "distinct_repeated_codes"
-- 7331--> hay 7331 codes repetidos, NO SE PUEDE USAR COMO ID

-- Generar id identificativo de cada fila
ALTER TABLE public."OFF_Viz_ini"
ADD COLUMN id SERIAL PRIMARY KEY;


-- Crear la tabla para los países
CREATE TABLE public."OFF_Viz__countries" (
    id INTEGER,
    created_datetime TIMESTAMP,
    country VARCHAR
);

-- Crear la tabla para las categorías
CREATE TABLE public."OFF_Viz__categories" (
    id INTEGER,
    created_datetime TIMESTAMP,
    category VARCHAR
);

-- Insertar datos desglosados en la tabla de países
INSERT INTO public."OFF_Viz__countries" (id, created_datetime, country)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(countries_en, ',')) AS country
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de categorías
INSERT INTO public."OFF_Viz__categories" (id, created_datetime, category)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(categories_en, ',')) AS category
FROM 
    public."OFF_Viz_ini";

--Obterner tipos de columnas principales para obtener datos globales
SELECT 
    column_name, 
    data_type
FROM 
    information_schema.columns
WHERE 
    table_schema = 'public' AND 
    table_name = 'OFF_Viz_ini' AND 
    column_name IN (
        'packaging_en',
        'labels_en',
        'allergens_en',
        'traces_en',
        'additives_en',
        'food_groups_en',
        'main_category_en',
        'energy-kcal_100g',
        'fat_100g',
        'saturated-fat_100g',
        'carbohydrates_100g',
        'cholesterol_100g',
        'sugars_100g',
        'added-sugars_100g',
        'fiber_100g',
        'proteins_100g',
        'salt_100g',
        'added-salt_100g'
    );

-- Crear la tabla para packaging
CREATE TABLE public."OFF_Viz__packaging" (
    id INTEGER,
    created_datetime TIMESTAMP,
    packaging TEXT
);

-- Crear la tabla para labels
CREATE TABLE public."OFF_Viz__labels" (
    id INTEGER,
    created_datetime TIMESTAMP,
    label TEXT
);

-- Crear la tabla para allergens
CREATE TABLE public."OFF_Viz__allergens" (
    id INTEGER,
    created_datetime TIMESTAMP,
    allergen DOUBLE PRECISION
);

-- Crear la tabla para traces
CREATE TABLE public."OFF_Viz__traces" (
    id INTEGER,
    created_datetime TIMESTAMP,
    trace TEXT
);

-- Crear la tabla para additives
CREATE TABLE public."OFF_Viz__additives" (
    id INTEGER,
    created_datetime TIMESTAMP,
    additive TEXT
);

-- Crear la tabla para food_groups
CREATE TABLE public."OFF_Viz__food_groups" (
    id INTEGER,
    created_datetime TIMESTAMP,
    food_group TEXT
);

-- Crear la tabla para main_category
CREATE TABLE public."OFF_Viz__main_category" (
    id INTEGER,
    created_datetime TIMESTAMP,
    main_category TEXT
);

-- Crear la tabla para energy-kcal_100g
CREATE TABLE public."OFF_Viz__energy_kcal_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    energy_kcal_100g DOUBLE PRECISION
);

-- Crear la tabla para fat_100g
CREATE TABLE public."OFF_Viz__fat_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    fat_100g DOUBLE PRECISION
);

-- Crear la tabla para saturated-fat_100g
CREATE TABLE public."OFF_Viz__saturated_fat_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    saturated_fat_100g DOUBLE PRECISION
);

-- Crear la tabla para carbohydrates_100g
CREATE TABLE public."OFF_Viz__carbohydrates_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    carbohydrates_100g DOUBLE PRECISION
);

-- Crear la tabla para cholesterol_100g
CREATE TABLE public."OFF_Viz__cholesterol_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    cholesterol_100g DOUBLE PRECISION
);

-- Crear la tabla para sugars_100g
CREATE TABLE public."OFF_Viz__sugars_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    sugars_100g DOUBLE PRECISION
);

-- Crear la tabla para added-sugars_100g
CREATE TABLE public."OFF_Viz__added_sugars_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    added_sugars_100g DOUBLE PRECISION
);

-- Crear la tabla para fiber_100g
CREATE TABLE public."OFF_Viz__fiber_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    fiber_100g DOUBLE PRECISION
);

-- Crear la tabla para proteins_100g
CREATE TABLE public."OFF_Viz__proteins_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    proteins_100g DOUBLE PRECISION
);

-- Crear la tabla para salt_100g
CREATE TABLE public."OFF_Viz__salt_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    salt_100g DOUBLE PRECISION
);

-- Crear la tabla para added-salt_100g
CREATE TABLE public."OFF_Viz__added_salt_100g" (
    id INTEGER,
    created_datetime TIMESTAMP,
    added_salt_100g DOUBLE PRECISION
);

-- Insertar datos desglosados en la tabla de packaging
INSERT INTO public."OFF_Viz__packaging" (id, created_datetime, packaging)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(packaging_en, ',')) AS packaging
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de labels
INSERT INTO public."OFF_Viz__labels" (id, created_datetime, label)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(labels_en, ',')) AS label
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de allergens
INSERT INTO public."OFF_Viz__allergens" (id, created_datetime, allergen)
SELECT 
    id, 
    created_datetime,
    allergens_en::DOUBLE PRECISION AS allergen
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de traces
INSERT INTO public."OFF_Viz__traces" (id, created_datetime, trace)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(traces_en, ',')) AS trace
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de additives
INSERT INTO public."OFF_Viz__additives" (id, created_datetime, additive)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(additives_en, ',')) AS additive
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de food_groups
INSERT INTO public."OFF_Viz__food_groups" (id, created_datetime, food_group)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(food_groups_en, ',')) AS food_group
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de main_category
INSERT INTO public."OFF_Viz__main_category" (id, created_datetime, main_category)
SELECT 
    id, 
    created_datetime,
    unnest(string_to_array(main_category_en, ',')) AS main_category
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de energy-kcal_100g
INSERT INTO public."OFF_Viz__energy_kcal_100g" (id, created_datetime, energy_kcal_100g)
SELECT 
    id, 
    created_datetime,
    "energy-kcal_100g"::DOUBLE PRECISION AS energy_kcal_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de fat_100g
INSERT INTO public."OFF_Viz__fat_100g" (id, created_datetime, fat_100g)
SELECT 
    id, 
    created_datetime,
    fat_100g::DOUBLE PRECISION AS fat_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de saturated-fat_100g
INSERT INTO public."OFF_Viz__saturated_fat_100g" (id, created_datetime, saturated_fat_100g)
SELECT 
    id, 
    created_datetime,
    "saturated-fat_100g"::DOUBLE PRECISION AS saturated_fat_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de carbohydrates_100g
INSERT INTO public."OFF_Viz__carbohydrates_100g" (id, created_datetime, carbohydrates_100g)
SELECT 
    id, 
    created_datetime,
    carbohydrates_100g::DOUBLE PRECISION AS carbohydrates_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de cholesterol_100g
INSERT INTO public."OFF_Viz__cholesterol_100g" (id, created_datetime, cholesterol_100g)
SELECT 
    id, 
    created_datetime,
    cholesterol_100g::DOUBLE PRECISION AS cholesterol_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de sugars_100g
INSERT INTO public."OFF_Viz__sugars_100g" (id, created_datetime, sugars_100g)
SELECT 
    id, 
    created_datetime,
    sugars_100g::DOUBLE PRECISION AS sugars_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de added-sugars_100g
INSERT INTO public."OFF_Viz__added_sugars_100g" (id, created_datetime, added_sugars_100g)
SELECT 
    id, 
    created_datetime,
    "added-sugars_100g"::DOUBLE PRECISION AS added_sugars_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de fiber_100g
INSERT INTO public."OFF_Viz__fiber_100g" (id, created_datetime, fiber_100g)
SELECT 
    id, 
    created_datetime,
    fiber_100g::DOUBLE PRECISION AS fiber_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de proteins_100g
INSERT INTO public."OFF_Viz__proteins_100g" (id, created_datetime, proteins_100g)
SELECT 
    id, 
    created_datetime,
    proteins_100g::DOUBLE PRECISION AS proteins_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de salt_100g
INSERT INTO public."OFF_Viz__salt_100g" (id, created_datetime, salt_100g)
SELECT 
    id, 
    created_datetime,
    salt_100g::DOUBLE PRECISION AS salt_100g
FROM 
    public."OFF_Viz_ini";

-- Insertar datos desglosados en la tabla de added-salt_100g
INSERT INTO public."OFF_Viz__added_salt_100g" (id, created_datetime, added_salt_100g)
SELECT 
    id, 
    created_datetime,
    "added-salt_100g"::DOUBLE PRECISION AS added_salt_100g
FROM 
    public."OFF_Viz_ini";

-- Contar cuantas entradas hay de cada uno de los países
SELECT country, COUNT(*) AS count
FROM public."OFF_Viz__countries"
GROUP BY country
ORDER BY count DESC;

-- Crear tabla nueva para unificar countries y mapear la tabla de countries existente con valores unificados
CREATE TABLE public.CountryMapping (
    variant_name VARCHAR(255),
    unified_name VARCHAR(255)
);

INSERT INTO public.CountryMapping (variant_name, unified_name) VALUES
('France', 'France'),
('Francia', 'France'),
('Frankreich', 'France'),
('fr:francia', 'France'),
('fr:frankrijk', 'France'),
('Deutschland', 'Germany'),
('Germany', 'Germany'),
('Allemagne', 'Germany'),
('Alemania', 'Germany'),
('de:allemagne', 'Germany'),
('de:autriche', 'Germany'),
('de:nÄ›mecko', 'Germany'),
('de:deut', 'Germany'),
('Spain', 'Spain'),
('España', 'Spain'),
('Espagne', 'Spain'),
('es:es-en-spain', 'Spain'),
('es:espagne', 'Spain'),
('Hiszpania', 'Spain'),
('Switzerland', 'Switzerland'),
('Schweiz', 'Switzerland'),
('Suisse', 'Switzerland'),
('Suiza', 'Switzerland'),
('de:suiza', 'Switzerland'),
('Belgium', 'Belgium'),
('Belgique', 'Belgium'),
('Belgien', 'Belgium'),
('België', 'Belgium'),
('nl:belgie', 'Belgium'),
('bg:belgique', 'Belgium'),
('United Kingdom', 'United Kingdom'),
('Royaume-Uni', 'United Kingdom'),
('United-kingdom-english', 'United Kingdom'),
('United-kingdom-ireland', 'United Kingdom'),
('Untied-kingdom', 'United Kingdom'),
('bg:angleterre', 'United Kingdom'),
('bg:angle', 'United Kingdom'),
('England', 'United Kingdom'),
('United States', 'United States'),
('États-Unis', 'United States'),
('Estados-unidos', 'United States'),
('en:united-states', 'United States'),
('de:vereinigte-staaten-von-amerika', 'United States'),
('On-canada-n6a-4z2', 'United States'),
('Italy', 'Italy'),
('Italia', 'Italy'),
('Italie', 'Italy'),
('Canada', 'Canada'),
('CA', 'Canada'),
('en:canada', 'Canada'),
('Made-in-canada-from-domestic-and-imported-ingredients', 'Canada'),
('Product-of-usa-packed-in-canada-imported-by-strong-international-trading-inc-richmond-bc-www-siti-ca', 'Canada'),
('Morocco', 'Morocco'),
('Maroc', 'Morocco'),
('Ø§Ù„Ù…ØºØ±Ø¨', 'Morocco'),
('Côte d''Ivoire', 'Ivory Coast'),
('Ivory Coast', 'Ivory Coast'),
('Netherlands', 'Netherlands'),
('Niederlande', 'Netherlands'),
('Holanda', 'Netherlands'),
('Russia', 'Russia'),
('Россия', 'Russia'),
('Japan', 'Japan'),
('日本', 'Japan');

ALTER TABLE public."OFF_Viz__countries"
ADD COLUMN unified_country VARCHAR(255);

UPDATE public."OFF_Viz__countries" AS target
SET unified_country = mapping.unified_name
FROM public.CountryMapping AS mapping
WHERE target.country = mapping.variant_name;

SELECT DISTINCT unified_country
FROM public."OFF_Viz__countries"
ORDER BY unified_country;

-- los valores a null de unified sean iguales a los de country
UPDATE public."OFF_Viz__countries"
SET unified_country = COALESCE(unified_country, country);

SELECT unified_country, COUNT(*) AS count
FROM public."OFF_Viz__countries"
GROUP BY unified_country
ORDER BY count DESC;


-- Paso 1: Identificar los valores de `unified_country` con menos de 500 entradas
WITH CountryCounts AS (
    SELECT unified_country, COUNT(*) AS count
    FROM public."OFF_Viz__countries"
    GROUP BY unified_country
    HAVING COUNT(*) < 500
)

-- Paso 2: Eliminar las filas correspondientes a menos de 500 entradas
DELETE FROM public."OFF_Viz__countries" AS target
USING CountryCounts
WHERE target.unified_country = CountryCounts.unified_country;
-- DELETE 18936

SELECT COUNT(*) AS total_rows
FROM public."OFF_Viz__countries";
--3349422

--las 18,936 filas borradas suponen aproximadamente el 0.5621% del total de entradas

-- no existen valores en la columna allergens_en
SELECT allergens_en, COUNT(*) AS count
FROM public."OFF_Viz_ini"
GROUP BY allergens_en
ORDER BY count DESC;
-- null: 3211062

-- Crear la tabla para almacenar los promedios por mes-año y grupo de alimentos para generar predicciones
CREATE TABLE public."OFF_Viz__fg_monthly_avg" (
    year_month TEXT,
    food_groups_en TEXT,
    avg_energy_kcal_100g DOUBLE PRECISION,
    avg_fat_100g DOUBLE PRECISION,
    avg_saturated_fat_100g DOUBLE PRECISION,
    avg_carbohydrates_100g DOUBLE PRECISION,
    avg_added_sugars_100g DOUBLE PRECISION,
    avg_proteins_100g DOUBLE PRECISION,
    avg_added_salt_100g DOUBLE PRECISION
);

-- Insertar datos agrupados por mes-año en la nueva tabla
INSERT INTO public."OFF_Viz__fg_monthly_avg" (
    year_month,
    food_groups_en,
    avg_energy_kcal_100g,
    avg_fat_100g,
    avg_saturated_fat_100g,
    avg_carbohydrates_100g,
    avg_added_sugars_100g,
    avg_proteins_100g,
    avg_added_salt_100g
)
SELECT
    TO_CHAR(created_datetime, 'YYYY-MM') AS year_month,
    food_groups_en,
    AVG("energy-kcal_100g") FILTER (WHERE "energy-kcal_100g" IS NOT NULL AND "energy-kcal_100g" != 'NaN')::DOUBLE PRECISION AS avg_energy_kcal_100g,
    AVG("fat_100g") FILTER (WHERE "fat_100g" IS NOT NULL AND "fat_100g" != 'NaN')::DOUBLE PRECISION AS avg_fat_100g,
    AVG("saturated-fat_100g") FILTER (WHERE "saturated-fat_100g" IS NOT NULL AND "saturated-fat_100g" != 'NaN')::DOUBLE PRECISION AS avg_saturated_fat_100g,
    AVG("carbohydrates_100g") FILTER (WHERE "carbohydrates_100g" IS NOT NULL AND "carbohydrates_100g" != 'NaN')::DOUBLE PRECISION AS avg_carbohydrates_100g,
    AVG("added-sugars_100g") FILTER (WHERE "added-sugars_100g" IS NOT NULL AND "added-sugars_100g" != 'NaN')::DOUBLE PRECISION AS avg_added_sugars_100g,
    AVG("proteins_100g") FILTER (WHERE "proteins_100g" IS NOT NULL AND "proteins_100g" != 'NaN')::DOUBLE PRECISION AS avg_proteins_100g,
    AVG("added-salt_100g") FILTER (WHERE "added-salt_100g" IS NOT NULL AND "added-salt_100g" != 'NaN')::DOUBLE PRECISION AS avg_added_salt_100g
FROM
    public."OFF_Viz_ini"
WHERE
    "food_groups_en" IS NOT NULL AND "food_groups_en" != 'NaN'
GROUP BY
    TO_CHAR(created_datetime, 'YYYY-MM'),
    food_groups_en;
	
	