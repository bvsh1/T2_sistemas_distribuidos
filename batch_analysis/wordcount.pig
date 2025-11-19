-- Cargar los datos de entrada (ruta pasada por parámetro)
raw_data = LOAD '$INPUT_PATH' USING PigStorage('\n') AS (line:chararray);

-- 1. Tokenización: Separar cada respuesta en palabras
words = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- 2. Limpieza: Convertir a minúsculas
lower_words = FOREACH words GENERATE LOWER(word) AS word;

-- 3. Limpieza: Eliminar signos de puntuación (solo dejar letras a-z)
no_punct_words = FOREACH lower_words GENERATE REPLACE(word, '[^a-z]', '') AS word;
-- Eliminamos palabras que quedaron vacías tras limpiar
valid_words = FILTER no_punct_words BY word != '';

-- Cargar la lista de stopwords DESDE HDFS (usaremos ruta absoluta)
stopwords = LOAD '/wordcount/stopwords.txt' AS (stopword:chararray);

-- 4. Limpieza: Filtrar stopwords usando un LEFT JOIN (Anti-Join)
-- "Replicated" carga la tabla pequeña (stopwords) en memoria para que sea ultra rápido
joined_words = JOIN valid_words BY word LEFT, stopwords BY stopword USING 'replicated';

-- Nos quedamos solo con las filas donde NO hubo coincidencia en la derecha (stopword es NULL)
clean_joined = FILTER joined_words BY stopwords::stopword IS NULL;

-- Recuperamos solo la columna de la palabra
final_words = FOREACH clean_joined GENERATE valid_words::word AS word;

-- 5. Conteo: Agrupar por palabra
grouped_words = GROUP final_words BY word;

-- 6. Conteo: Calcular la frecuencia
word_count = FOREACH grouped_words GENERATE group AS word, COUNT(final_words) AS count;

-- 7. Ordenar los resultados de mayor a menor frecuencia
ordered_count = ORDER word_count BY count DESC;

-- Guardar los resultados en la carpeta de salida (ruta pasada por parámetro)
STORE ordered_count INTO '$OUTPUT_PATH' USING PigStorage('\t');