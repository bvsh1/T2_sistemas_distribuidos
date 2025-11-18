-- Cargar los datos de entrada (ruta pasada por parámetro)
raw_data = LOAD '$INPUT_PATH' USING PigStorage('\n') AS (line:chararray);

-- 1. Tokenización: Separar cada respuesta en palabras
words = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- 2. Limpieza: Convertir a minúsculas
lower_words = FOREACH words GENERATE LOWER(word) AS word;

-- 3. Limpieza: Eliminar signos de puntuación (solo dejar letras a-z)
-- ¡¡ESTA LÍNEA ES LA QUE CAMBIÓ!!
no_punct_words = FOREACH lower_words GENERATE REPLACE(word, '[^a-z]', '') AS word;

-- Cargar la lista de stopwords
stopwords_list = LOAD 'stopwords.txt' AS (stopword:chararray);

-- 4. Limpieza: Filtrar stopwords y palabras vacías
filtered_words = FILTER no_punct_words BY 
    (word != '') AND 
    (NOT (word IN stopwords_list));

-- 5. Conteo: Agrupar por palabra
grouped_words = GROUP filtered_words BY word;

-- 6. Conteo: Calcular la frecuencia de cada palabra
word_count = FOREACH grouped_words GENERATE group AS word, COUNT(filtered_words) AS count;

-- 7. Ordenar los resultados de mayor a menor frecuencia
ordered_count = ORDER word_count BY count DESC;

-- Guardar los resultados en la carpeta de salida (ruta pasada por parámetro)
STORE ordered_count INTO '$OUTPUT_PATH' USING PigStorage('\t');