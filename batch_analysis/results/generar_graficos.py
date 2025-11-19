import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 1. Cargar los datos
print("Cargando datos...")
# Asumimos nombres de columnas 'word' y 'count'
df_human = pd.read_csv('humanas_wordcount.txt', sep='\t', names=['word', 'count'])
df_llm = pd.read_csv('llm_wordcount.txt', sep='\t', names=['word', 'count'])

# --- GRÁFICO 1: Comparación de Top 20 Palabras (Barras) ---
def plot_top_words(df, title, color, filename):
    plt.figure(figsize=(10, 6))
    # Tomamos las top 20
    top_data = df.head(20).sort_values(by='count', ascending=True)
    
    plt.barh(top_data['word'], top_data['count'], color=color)
    plt.title(title)
    plt.xlabel('Frecuencia')
    plt.ylabel('Palabra')
    plt.tight_layout()
    plt.savefig(filename)
    print(f"Gráfico guardado: {filename}")
    plt.close()

print("Generando gráficos de barras...")
plot_top_words(df_human, 'Top 20 Palabras - Respuestas Humanas (Yahoo!)', 'skyblue', 'top20_humanas.png')
plot_top_words(df_llm, 'Top 20 Palabras - Respuestas LLM', 'salmon', 'top20_llm.png')


# --- GRÁFICO 2: Nubes de Palabras (WordClouds) ---
def generate_wordcloud(df, title, filename):
    # WordCloud necesita un diccionario de {palabra: frecuencia}
    data_dict = dict(zip(df['word'], df['count']))
    
    wc = WordCloud(width=800, height=400, background_color='white', max_words=100).generate_from_frequencies(data_dict)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"Nube de palabras guardada: {filename}")
    plt.close()

print("Generando nubes de palabras...")
# Filtramos nulos por si acaso
generate_wordcloud(df_human.dropna(), 'Nube de Palabras - Humanas', 'wordcloud_humanas.png')
generate_wordcloud(df_llm.dropna(), 'Nube de Palabras - LLM', 'wordcloud_llm.png')

print("\n¡Listo! Revisa los archivos .png generados.")